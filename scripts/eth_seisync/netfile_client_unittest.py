import unittest

import netfile_client as Form700_Blocking_Class


class Tests(unittest.TestCase):
    secrets = {'login': 'foo', 'password': 'bar'}
    sync_client = Form700_Blocking_Class.Form700_Blocking(credentials=secrets)

    def testCreds(self):
        self.assertEqual(self.sync_client.credentials, {'UserName': 'foo',
                         'Password': 'bar'})

    def testInit(self):
        pass


if __name__ == '__main__':
    unittest.main()
