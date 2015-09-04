#Open dataset names hardcoded in the program are retrived from following sources-
#Supermarkets - http://geolytix.co.uk/blog/tag/supermarkets/
#Train stations  - http://data.gov.uk/dataset/naptan
#Schools  - http://www.education.gov.uk/edubase/home.xhtml
#GP Surgeries - http://data.gov.uk/dataset/england-nhs-connecting-for-health-organisation-data-service-data-files-of-general-medical-practices
#Postcodes and area codes - http://www.freemaptools.com/download-uk-postcode-lat-lng.htm
#Postcodes - http://www.doogal.co.uk/UKPostcodes.php?
#Cloud storage library reference - https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/download
#Program reference - https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/getstarted

import logging
import os
import csv
import cloudstorage as gcs
import webapp2

from google.appengine.api import app_identity

# Retry param class reference - https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/retryparams_class
# Retry can help overcome transient urlfetch or GCS issues, such as timeouts.
my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)
gcs.set_default_retry_params(my_default_retry_params)


class MainPage(webapp2.RequestHandler):

	def get(self):
		bucket_name = os.environ.get('local-amenities.appspot.com', app_identity.get_default_gcs_bucket_name())
		self.response.headers['Content-Type'] = 'text/plain'
		self.response.write('Demo GCS Application running from Version: '
                        + os.environ['CURRENT_VERSION_ID'] + '\n')
		self.response.write('Using bucket name: ' + bucket_name + '\n\n')
		bucket = '/' + bucket_name
		filename = bucket + '/G_postcodes_2.csv'
		self.tmp_filenames_to_clean_up = []
		
		try:
			self.create_file(filename, 'G_postcodes_2.csv')
			self.read_file(filename)
			self.response.write('\n\n')

		except Exception, e:  
			logging.exception(e)
			self.response.write('\n\nThere was an error running the demo! '
								'Please check the logs for more details.\n')
 

	def create_file(self, filename, local_file):
		self.response.write('Creating file %s\n' % filename)
		write_retry_params = gcs.RetryParams(backoff_factor=1.1)
		gcs_file = gcs.open(filename,
							'w',
							content_type='text/csv',
							retry_params=write_retry_params)
		f=open(local_file)
		reader=csv.reader(f)
		writer = csv.writer(gcs_file)
		for row in reader:        
			writer.writerow(row)
		gcs_file.close()
		self.tmp_filenames_to_clean_up.append(filename)

	def read_file(self, filename):
		self.response.write('Abbreviated file content (first line and last 1K):\n')
		gcs_file = gcs.open(filename)
		self.response.write(gcs_file.readline())
		gcs_file.seek(-1024, os.SEEK_END)
		self.response.write(gcs_file.read())
		gcs_file.close()

app = webapp2.WSGIApplication([('/', MainPage)],
                              debug=True)