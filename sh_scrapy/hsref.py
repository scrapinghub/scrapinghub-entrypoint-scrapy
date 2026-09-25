"""
Module to hold a reference to singleton Hubstorage client and Job instance
"""
import os
import warnings
from codecs import decode

from scrapy.utils.python import to_unicode


class _HubstorageRef(object):

    def __init__(self):
        self.enabled = 'SHUB_JOBKEY' in os.environ
        self._client = None
        self._project = None
        self._job = None
        if self.enabled:
            self.jobkey = os.environ['SHUB_JOBKEY']
            job_id = [int(id) for id in self.jobkey.split('/')]
            self._projectid, self._spiderid, self._jobcounter = job_id
        else:
            self._projectid = None
            self._spiderid = None
            self._jobcounter = None

    @property
    def auth(self):
        return to_unicode(decode(os.environ['SHUB_JOBAUTH'], 'hex_codec'))

    @property
    def endpoint(self):
        return os.environ.get('SHUB_STORAGE')

    @property
    def projectid(self):
        return self._projectid

    @property
    def spiderid(self):
        return self._spiderid

    @property
    def jobid(self):
        return self._jobcounter

    @property
    def client(self):
        from scrapinghub import ScrapinghubClient
        if self._client is None:
            user_agent = os.environ.get('SHUB_HS_USER_AGENT')
            with warnings.catch_warnings():
                # Job credentials are all a job has, and they cover the
                # storage API that hsref is for.
                warnings.filterwarnings(
                    'ignore',
                    message='A lot of endpoints support authentication only via apikey',
                    category=UserWarning,
                )
                self._client = ScrapinghubClient(self.auth,
                                                 endpoint=self.endpoint,
                                                 user_agent=user_agent)
        return self._client._hsclient

    @property
    def project(self):
        if self._project is None:
            self._project = self.client.get_project(str(self.projectid))
        return self._project

    @property
    def job(self):
        if self._job is None:
            self._job = self.project.get_job((self.spiderid, self.jobid))
        return self._job

    def close(self):
        if self._client is not None:
            self._client.close()

hsref = _HubstorageRef()
