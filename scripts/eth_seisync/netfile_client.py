import sys
import json
import re
import copy
import requests
import logging
from sodapy import Socrata


class Form700_Blocking:
    """
    Perform download of all Form 700 data

    'Blocking' because the intention is to write an async version
    """

    def __init__(self, credentials=None, socrata_config=None, schema_defs=None,
                 get_unredacted=False):

        self.logger = logging.getLogger(__name__)
        self.logger.info('****** Initialized Form 700 Netfile client ******')

        self.headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
        }

        self.authUrl = \
            'https://netfile.com:443/Connect2/api/authenticate'
        self.url_cover = \
            'https://netfile.com:443/Connect2/api/public/sei/export/cover'
        self.url_schedule = \
            'https://netfile.com:443/Connect2/api/public/sei/export/schedule'

        self.params = {
            'AgencyPrefix': 'SFO',
            'IsRedacted': 'true',
            'PageSize': 1000,
        }
        if get_unredacted:
            self.params['IsRedacted'] = False

        if credentials['netfile']:
            self.netfile_creds = {
                'UserName': credentials['netfile']['login'],
                'Password': credentials['netfile']['password'],

            }
        if credentials['socrata']:
            self.soda_creds = {
                'username': credentials['socrata']['keyId'],
                'password': credentials['socrata']['keySecret'],
            }
            self.appToken = credentials['socrata']['appToken']

        self.schedules = [
            'cover',
            'comments',
            'scheduleA1',
            'scheduleA2',
            'scheduleB',
            'scheduleC',
            'scheduleD',
            'scheduleE',
        ]

        # TODO validate schema definitions
        self.schemas = schema_defs
        self.data = {}
        self.was_reset = {}
        self.date_columns = {}
        for schedule in self.schedules:
            self.date_columns[schedule] = []
            for col in self.schemas[schedule]:
                if self.schemas[schedule][col] == 'date':
                    self.date_columns[schedule].append(col)

            # for temporary "database" of extracted & transformed data
            self.data[schedule] = []
            # to keep track of the DataSF dataset being reset
            self.was_reset[schedule] = False

        # TODO validate socrata_config
        self.socrata_config = socrata_config

        self.filer_cols = ['filingId', 'filerName', 'departmentName',
                           'positionName', 'offices', 'periodStart',
                           'periodEnd', 'filingDate']
        self.filings = {}

        # total: total number of rows on Netfile
        # received: rows received from Netfile
        # send: rows to send to Socrata
        # created: rows created on Socrata
        self.sanity_check = {k: {'received': 0, 'send': 0, 'created': 0}
                             for k in self.schedules}
        self.sanity_check['cover'] = {'total': 0, 'received': 0, 'send': 0,
                                      'created': 0}
        self.sanity_check['items'] = {'total': 0, 'received': 0, 'send': 0,
                                      'created': 0}

    def stop_all_execution(self, msg):
        """
        generic error handler, logs and then halts execution
        """
        if isinstance(msg, Exception):
            self.logger.exception(msg)
        else:
            self.logger.error(f'halting execution: {msg}')
        sys.exit(1)

    def authenticate(self, session):
        """
        authenticate against the session
        """
        self.logger.info(f'Making authentication request')
        return session.post(self.authUrl, data=self.netfile_creds,
                            headers=self.headers)

    @staticmethod
    def castDate(date):
        """
        deal with the fact that Socrata wants style YYYY-MM-DDTHH:mm:ss,
        Netfile provides style YYYY-MM-DDTHH:mm:ss.fffffff-HH:mm,
        and Python datetime deals with style YYYY-MM-DDTHH:mm:ss.f-HHmm,
        and also, some fields might have user-input mm/dd/yyyy format
        """
        regex = re.compile(r'^(\d+)\/(\d+)\/(\d{4})$')

        if date is None:
            return None
        elif date == '':
            return None
        elif regex.match(date):
            mdy = regex.match(date)
            month = mdy.group(1).zfill(2)
            day = mdy.group(2).zfill(2)
            year = mdy.group(3)
            return f'{year}-{month}-{day}'
        elif len(date) != 33:
            return date
        else:
            # TODO: make sure that socrata actually converts timezones
            # correctly
            return date[:19]  # +date[27:]

    @staticmethod
    def flattenOffices(offices):
        """
        takes a list, flattens it, returns a string
        use for the "offices" portion of a cover item
        """
        as_a_list = list(map(lambda office:
                         f"{office['filerPosition']} - \
                            {office['filerDivisionBoardDistrict']}", offices))
        return ' '.join(as_a_list)

    @staticmethod
    def flattenIncomeSources(income_sources):
        """
        takes a list, flattens it, returns a string
        use for schedules A2, B, and C
        """
        as_a_list = list(map(lambda income_source: f"{income_source['name']}",
                         income_sources))
        return '|'.join(as_a_list)

    def explodeScheduleA2(self, item):
        """
        run flattenoffices/incomesources first
        """
        res = []
        if len(item['realProperties']) == 0:
            item.update({
                'realProperty_investmentType': None,
                'realProperty_businessName': None,
                'realProperty_parcelAddress': None,
                'realProperty_descriptionOrCityOrLocation': None,
                'realProperty_dateAcquired': None,
                'realProperty_dateDisposed': None,
                'realProperty_fairMarketValue': None,
                'realProperty_fairMarketValueAsRange': None,
                'realProperty_natureOfInterest': None,
                'realProperty_natureOfInterest_LeaseYearsRemaining': None,
                'realProperty_natureOfInterest_OtherDescription': None,
            })
            del item['realProperties']
            item = self.pickKeys(item, 'scheduleA2')
            res.append(item)
        else:
            for property in item['realProperties']:
                newitem = copy.deepcopy(item)
                for k, v in property.items():
                    newitem[f"realProperty_{k}"] = v
                del newitem['realProperties']
                newitem = self.pickKeys(newitem, 'scheduleA2')
                res.append(newitem)
        return res

    def explodeScheduleB(self, item):
        for k, v in item['loan'].items():
            item[f"loan_{k}"] = v
        del item['loan']
        item = self.pickKeys(item, 'scheduleB')
        return item

    def explodeScheduleD(self, item):
        """
        run flattenoffices/incomesources first
        """
        res = []
        new_props = ['amount', 'description', 'giftDate']
        for gift in item['gifts']:
            newitem = copy.deepcopy(item)
            for k in new_props:
                newitem[k] = gift[k]
            del newitem['gifts']
            newitem = self.pickKeys(newitem, 'scheduleD')
            res.append(newitem)
        return res

    def deal_with_dates(self, schedule_type, item):
        """
        helper function to translate identified date columns
        """
        for col in self.date_columns[schedule_type]:
            item[col] = self.castDate(item[col])
        return item

    def pickKeys(self, item, schedule_type):
        """
        choose the keys for columns on datasf
        """
        datasf_keys = self.schemas[schedule_type].keys()
        item = {k: item[k] for k in datasf_keys}
        return item

    def extractData(self, session, params, schedule_type):
        """
        request Coversheet or Schedule data from the api
        """
        self.logger.debug(f'Requesting {schedule_type} data. params: {params}')
        if schedule_type == 'cover':
            url = self.url_cover
        else:
            url = self.url_schedule
        try:
            response = session.post(url, params=params, headers=self.headers)
            if response.status_code not in [200, 201]:
                raise Exception(
                    f'Error requesting Url: {url}, Response code: \
                    {response.status_code}. \
                    Error Message: {response.text}')
        except Exception as ex:
            self.stop_all_execution(ex)
        return response

    def extractConfirmCover(self):
        """
        make sure we got what we expected
        """
        expected = self.sanity_check['cover']['total']
        received = self.sanity_check['cover']['received']
        self.logger.debug(f'Expected {expected} cover items, got {received}')
        return expected == received

    def extractConfirmSchedule(self):
        """
        make sure we got what we expected
        """
        expected = self.sanity_check['items']['total']
        received = self.sanity_check['items']['received']
        self.logger.debug(
            f'Expected {expected} total schedule items, got {received}')
        for schedule_type in self.schedules:
            if schedule_type == 'cover':
                continue
            received_schedule = self.sanity_check[schedule_type]['received']
            self.logger.debug(f'     Got {received_schedule} items of '
                              f'{schedule_type}')
        return expected == received

    def transformCoverResponse(self, response_json, page_num, url=None):
        """
        deal with the json response from a request to the cover-api
        each cover response represents a form 700 filing
        """
        data = []
        msg = f'Handling cover response'
        if url:
            msg += f' from {url}'
        self.logger.debug(msg)

        if (page_num == 1):
            self.sanity_check['cover']['total'] = \
                response_json['totalMatchingCount']

        # format filing for DataSF
        for row in response_json['filings']:
            # first deal with date string formatting
            for col in self.date_columns['cover']:
                row[col] = self.castDate(row[col])

            # flatten the offices object
            row['offices'] = str(self.flattenOffices(row['offices']))

            # add filing to filing_dictionary for joining to Schedule_Items
            row_dict = {}
            for key in self.filer_cols:
                row_dict[key] = row[key]
            self.filings[row['filingId']] = row_dict

            # choose only the keys we want
            datasf_keys = self.schemas['cover'].keys()
            desired = {k: row[k] for k in datasf_keys}
            data.append(desired)

        # add the total to the sanity_checker
        self.sanity_check['cover']['received'] += len(data)
        # we don't explode any cover rows, so #received == #send
        self.sanity_check['cover']['send'] += len(data)

        # add to temporary "database"
        self.saveResponseTemp('cover', data)
        return None

    def transformScheduleResponse(self, response_json, page_num, url=None):
        """
        deal with the json response from a request to the schedule-api

        schedule items are separated by schedule,
        sorted in reverse chronological order by filing
        """
        msg = f'Handling schedule response'
        if url:
            msg += f' from {url}'
        msg += f', page {page_num}'
        self.logger.debug(msg)

        for schedule_type in self.schedules:
            if schedule_type == 'cover':
                continue
            if (page_num == 1):
                self.sanity_check['items']['total'] = \
                    response_json['totalMatchingCount']
            if len(response_json[schedule_type]) > 0:
                received_items = len(response_json[schedule_type])
                self.sanity_check[schedule_type]['received'] += received_items
                self.sanity_check['items']['received'] += received_items
                data = []
                datasf_keys = self.schemas[schedule_type].keys()
                dataset = self.socrata_config[schedule_type]
                # prepare schedule types
                for item in response_json[schedule_type]:
                    # join with filing data
                    try:
                        item.update(self.filings[item['filingId']])
                    except KeyError as ex:
                        self.stop_all_execution(
                            f'Missing a filing record for id: {ex}')
                    except Exception as ex:
                        self.stop_all_execution(ex)

                    # flatten some schedule data
                    if schedule_type in ['scheduleA2', 'scheduleB',
                                         'scheduleC']:
                        item['incomeSources'] = self.flattenIncomeSources(
                            item['incomeSources']
                        )

                    # format filing for DataSF
                    # cast dates
                    # add to data object
                    if schedule_type == 'scheduleA2':
                        items = self.explodeScheduleA2(item)
                        for i in items:
                            i = self.deal_with_dates('scheduleA2', i)
                            data.append(i)
                    elif schedule_type == 'scheduleB':
                        item = self.explodeScheduleB(item)
                        item = self.deal_with_dates('scheduleB', item)
                        data.append(item)
                    elif schedule_type == 'scheduleD':
                        items = self.explodeScheduleD(item)
                        for i in items:
                            i = self.deal_with_dates('scheduleD', i)
                            data.append(i)
                    else:
                        item = self.pickKeys(item, schedule_type)
                        item = self.deal_with_dates(schedule_type, item)
                        data.append(item)
                self.sanity_check[schedule_type]['send'] += len(data)
                self.sanity_check['items']['send'] += len(data)
                # add to temporary "database"
                self.saveResponseTemp(schedule_type, data)
        self.logger.debug(f'Transformed page {page_num}')
        return None

    def saveResponseTemp(self, schedule_type, data):
        """
        save to temporary "database"
        """
        self.data[schedule_type].extend(data)
        return None

    def sendToDataSF(self, soda_client, dataset, data, was_reset=True):
        """
        send the update request to DataSF
        """
        # TODO: implement retry, make requests after 1st async
        self.logger.debug(f'Sending to DataSF, id: {dataset}')
        if was_reset is False:
            res = soda_client.replace(dataset, data, content_type='json')
        else:
            res = soda_client.upsert(dataset, data, content_type='json')
        return res

    def loadData(self, soda_client, schedule_type):
        """
        chunk and load data to Socrata
        """
        dataset = self.socrata_config[schedule_type]
        self.logger.debug(
            f'Starting to load {schedule_type}, sending to {dataset}')
        data = self.data[schedule_type]
        chunks = [data[x:x + 1000] for x in range(0, len(data), 1000)]
        for chunk in chunks:
            try:
                reset = self.was_reset[schedule_type]
                res = self.sendToDataSF(soda_client, dataset, chunk, reset)
            except Exception as ex:
                self.stop_all_execution(ex)
            self.logger.debug(f'response from DataSF id: {dataset} {res}')

            self.was_reset[schedule_type] = True

            # check for errors from socrata
            if (res['Errors'] > 0):
                self.stop_all_execution(
                    f'Error updating {dataset} with page {page_num} - {res}')

            # add totals to sanity_checker
            self.sanity_check[schedule_type]['created'] += res['Rows Created']
            if schedule_type != 'cover':
                self.sanity_check['items']['created'] += res['Rows Created']
        self.logger.debug(f'Sent all chunks for {schedule_type}')
        return None

    def loadConfirm(self, schedule_type):
        """
        make sure we loaded what we expected
        """
        received = self.sanity_check[schedule_type]['received']
        send = self.sanity_check[schedule_type]['send']
        created = self.sanity_check[schedule_type]['created']
        self.logger.info(
            f'Received {received} {schedule_type} items, sent {send}'
            f', created {created}')
        return send == created

    def logPageCounts(self, schedule_type, data):
        """
        write to the log how many pages we expect
        """
        total_pages = data['totalMatchingPages']
        total_records = data['totalMatchingCount']
        self.logger.info(
            f'Total pages in {schedule_type} datasets {total_pages}')
        self.logger.info(f'Total {schedule_type} records {total_records}')
        return None

    def sync(self):
        """
        Do all the syncing
        use a single client session to communicate with Socrata
        use a single Session object to communicate with Netfile
        """
        # Socrata client context manager
        with Socrata("data.sfgov.org",
                     self.appToken, **self.soda_creds) as soda_client:
            # get the data from NetFile
            with requests.Session() as session:
                # first, authenticate
                auth = self.authenticate(session)
                try:
                    if auth.status_code not in [200, 201, 202]:
                        raise Exception('Error authenticating. Error Message: '
                                        f'{auth.text}')
                except Exception as ex:
                    self.stop_all_execution(ex)

                # start to extract cover pages
                current_page = 1
                params = {'CurrentPageIndex': current_page}
                params.update(self.params)
                cover_page = self.extractData(session, params, 'cover')
                data = cover_page.json()
                self.logPageCounts('cover', data)
                self.transformCoverResponse(data, current_page, cover_page.url)

                # extract all the cover pages
                current_page += 1
                total_pages = data['totalMatchingPages']
                while current_page <= total_pages:
                    params['CurrentPageIndex'] = current_page
                    cover_page = self.extractData(session, params, 'cover')
                    self.transformCoverResponse(cover_page.json(),
                                                current_page, cover_page.url)
                    current_page += 1

                # check we extracted all the pages
                self.logger.info(
                    "Finished extracting covers "
                    f"{json.dumps(self.sanity_check['cover'])}")
                cover_extract_success = self.extractConfirmCover()
                if cover_extract_success is False:
                    self.stop_all_execution(
                        'Did not receive expected number of cover items')

                # load the cover data (send to DataSF)
                self.loadData(soda_client, 'cover')
                cover_load_success = self.loadConfirm('cover')
                if cover_load_success is False:
                    self.stop_all_execution(
                        'Did not load expected number of cover items')

                # start to extract schedules
                current_page = 1
                params['CurrentPageIndex'] = current_page
                schedule_page = self.extractData(session, params, 'schedule')
                data = schedule_page.json()
                self.logPageCounts('schedule', data)
                self.transformScheduleResponse(data, current_page,
                                               schedule_page.url)

                # extract all the schedule pages
                current_page += 1
                total_pages = data['totalMatchingPages']
                while current_page < total_pages:
                    params['CurrentPageIndex'] = current_page
                    schedule_page = self.extractData(session, params,
                                                     'schedule')
                    data = schedule_page.json()
                    self.transformScheduleResponse(data, current_page,
                                                   schedule_page.url)
                    current_page += 1
                params['CurrentPageIndex'] = current_page
                schedule_page = self.extractData(session, params, 'schedule')
                data = schedule_page.json()
                self.transformScheduleResponse(data, current_page,
                                               schedule_page.url)

                # check we extracted all the schedule pages
                self.logger.info(
                    f"Finished schedule extracts {self.sanity_check['items']}")
                schedule_extract_success = self.extractConfirmSchedule()
                if schedule_extract_success is False:
                    self.stop_all_execution(
                        'Did not receive expected number of schedule items')

            # start loading schedule data (without the Netfile Session)
            for schedule_type in self.schedules:
                if schedule_type == 'cover':
                    continue
                data = self.data[schedule_type]
                self.loadData(soda_client, schedule_type)
                self.loadConfirm(schedule_type)
            self.logger.info('Finished loading schedule data')
            schedule_load_success = self.loadConfirm('items')
            if schedule_load_success is False:
                self.stop_all_execution(
                    'Did not load expected number of schedule items')

        self.logger.info('****** Finished script execution ******')
