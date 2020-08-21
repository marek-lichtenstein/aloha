import sys
from files.dataproc import DataInterface, Modify
from files.db_tools import DbInterface
from files.utilities import single_download, download_many


class Setup:
	def __init__(self, *args, fp='files/persons.json', dl_buffer=5, **kwargs):
		self.fp = fp
		self.dl_buffer = dl_buffer
		self._store = DataInterface()
		self._db_api = DbInterface(*self._db_init_data())
		
	def _db_init_data(self):
		try:
			raw_sample_data = single_download(verbose=False)
			loaded_data = DataInterface.load_raw_data(raw_sample_data)
			return Modify.get_schema_data(loaded_data, self.store)
		except Exception as err:
			return err.msg

	def buffer_ctrl(func):
		def _inner(self, *args, **kwargs):
			downloader = func(self, *args, **kwargs)
			packets = self._consume(downloader)
			flag = 1
			while flag != 1:
				with self.store as store:
					while packets:
						data = next(packets)
					store.add_from_multi_stream(self._consume(downloader))
					self.db_api.insert_bulked_data(store.mappings)
			return
		return _inner

	def _consume(self, producer):
		pass

	def insert_many(self, size):
			for packet in download_many(size):
				if isinstance(packet, int) and packet < 0:
					sys.exit(0)
				with self.store as store:
					store.add_from_single_stream(packet)
					self.db_api.insert_bulked_data(store.mappings)
			return
	
	def insert_one(self, size):
			data = single_download(size)
			if isinstance(data, int) < 0:
				sys.exit(0)	
			with self.store as store:
				store.add_from_single_stream(data)
				self.db_api.insert_bulked_data(store.mappings)
			return

	def insert_downloaded_data(self, size):
		if size > 100000:
			print("Max size is 100.000 records!")
			sys.exit(0)
		if size > 5000: 
			self.insert_many(size)
		else:
			self.insert_one(size)
		return self

	def insert_data_from_file(self):
		with self.store.add_data_ff(self.fp) as store:
			self.db_api.insert_bulked_data(store.mappings)	
		return self

	@property
	def store(self):
		return self._store

	@property
	def db_api(self):
		if not self._db_api:
			self._db_api = DBInterface(self.store, self.url)
			self.db_api.create_db()
			return self._db_api
		return self._db_api

	def average_age(self, gender):
		return self.db_api.avg_age(gender)

	def gender_pct(self, gender):
		return self.db_api.queries.gender_pct(gender)
	
	def most_freq_pwds(self, limit):
		return self.db_api.most_freq_pwds(limit)
	
	def most_freq_cities(self, limit):
		return self.db_api.most_freq_cities(limit)
	
	def safest_pwd(self):
		return self.db_api.safest_pwd()
	
	def range_dob(self, start, end):
		return self.db_api.range_dob(start, end)
