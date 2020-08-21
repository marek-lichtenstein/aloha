from collections import defaultdict
from datetime import date
import json
import sys

class Modify:
	config = {
			'excluded': ['picture'],
			'mods': {
				'phone': lambda num: "".join(filter(lambda char: char.isdigit(), num)),
				'cell': lambda num: "".join(filter(lambda char: char.isdigit(), num))
				},
			'swap_names': {'id': 'userid'},
			}

	@classmethod
	def modify_many(cls, data, store):	
		for dataset in data:
			modified_data = Modify.modify(dataset, store, **cls.config)	
			store.append(modified_data)
		return store
	
	@classmethod
	def get_schema_data(cls, data, store):
		Modify.modify_many(data, store)
		store.clear()
		return store.construction_kit

	@staticmethod
	def modify(data, store, *args, **kwargs):		
		storage = Storage(store.datatypes)								  
		def _iterate(data, inner_keys=[], *args, **kwargs):	
			nonlocal storage
			for name in data:
				# modifications
				if name in kwargs.get('excluded', []):
					continue
				if name == 'dob':
					data[name].update({'dtb': Modify.dtb(data[name]['date'])})
				if name in kwargs.get('mods', []) and isinstance(data[name], (int, str)):
					data[name] = kwargs['mods'][name](data[name])

				# iterate over nested dictionary	
				if isinstance(data[name], dict):
					inner_keys.append(name)
					storage.add_data(name, dict())
					_iterate(data[name], inner_keys, *args, **kwargs)
				else:
					# check if it's nested data
					if inner_keys:
						storage.update_data(inner_keys[-1], {name: data[name]})	
					# nest top level key/value pairs
					else:
						store._add_tablename(name)
						storage.add_data(name, {name: data[name]}) 

			# keep track of table names
			if inner_keys:
				store._add_tablename(inner_keys.pop())
			# swap names
			if kwargs.get('swap_names'):
				for name in kwargs['swap_names']:
					if name in storage:
						old, new = name, kwargs['swap_names'][name]
						storage.swap_key(old, new, storage.pop(old))
						store._swap_tablename(old, new)			
			return storage
		return _iterate(data, *args, **kwargs)
	
	@staticmethod
	def dtb(dob):	
		is_leap = lambda _date: True if _date.year % 400 or _date.year % 4 else False
		today, dob = date.today(), date.fromisoformat(dob[:10])
		this_year_dob = date(today.year, dob.month, dob.day)
		if is_leap(dob):
			if is_leap(today):
				if today <= this_year_dob:
					return (this_year_dob - today).days
				else:
					next_dob = this_year_dob.replace(year=today.year + 4)
					return (next_dob - today).days
			else:
				next_leap_year = [year for year in range(today.year, today.year + 3) if is_leap(year)]
				next_dob = this_year_dob.replace(year=next_leap_year)
				return (next_dob - today).days
		if this_year_dob >= today:
			return (this_year_dob - today).days
		else:
			next_years_dob = this_year_dob.replace(year=today.year + 1)
			return (next_years_dob - today).days

		
class Types(set):
	"""Class type set used for storing datatypes."""
	def __init__(self):
		super().__init__()
	
	def __repr__(self):
		return super().__repr__()

	def __contains__(self, datatype):
		if super().__contains__(datatype):
			return True 
		elif datatype != str and str in self:
			raise ValueError('String contamination!')
		elif datatype not in (str, int):
			raise ValueError(f'Unsupported datatype: {datatype}!')
		else:
			return False 

	@property
	def datatype(self):
		if len(self) > 1:
			return str
		elif not self:
			raise ValueError("No datatype for this column")
		else:
			return self.copy().pop()

	def add_type(self, data):
		datatype = type(data)	
		try:	
			if datatype not in self:			
				self.add(datatype)
		except ValueError as err:
			self.clear()
			self.add(str)
		finally:
			return self
		

class TypesStorage(dict):
	"""Class type dict used to hold datatypes"""
	def __init__(self):
		super().__init__()
	
	def update_types(self, key, value):
		for col_name, val in value.items():
			if self[key].get(col_name):	
				self[key][col_name].add_type(val)
			else:
				self[key][col_name] = Types()
				self[key][col_name].add_type(val)
	
	def add_types(self, tablename, data):	
		if self.get(tablename):
			for col_name, value in data.items():
				if self[tablename].get(col_name):
					self[tablename][col_name].add_type(value)
				else:
					self[tablename][col_name] = Types()
					self[tablename][col_name].add_type(value)
		else:
			data = {col_name: Types().add_type(value) for col_name, value in data.items()}
			super().__setitem__(tablename, data)


class Storage(dict):
	"""class for holding table data and managing of datatypes updates."""
	def __init__(self, types):
		super().__init__()
		self.types = types	

	def add_data(self, key, data):
		super().__setitem__(key, data)
		self.types.add_types(key, data)


	def update_data(self, key, data):
		self[key].update(data)
		self.types.update_types(key, data)

	def swap_key(self, old_key, new_key, value):
		self[new_key] = value
		if self.types.get(old_key):
			self.types[new_key] = self.types.pop(old_key)
	
	@property
	def data(self):
		return self


class StorageContainer(list):
	"""Class for keeping multiple datasets"""
	def __init__(self):
		super().__init__()
		self.types = TypesStorage()
		self._tablenames = set()
		
	@property
	def tablenames(self):
		return list(self._tablenames)
	
	@property
	def datatypes(self):
		return self.types

	def _add_tablename(self, tablename):
		self._tablenames.add(tablename)
	
	def _swap_tablename(self, old, new):
		self._tablenames.remove(old)
		self._tablenames.add(new)

	def insert_data(self, data):
		Modify.modify_many(data, self)


class DataInterface(StorageContainer):
	def __init__(self):
		super().__init__()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.clear()

	@property	
	def mappings(self):
		mappings = defaultdict(list)
		for dataset in self:
			for tablename, mapping in dataset.items():
				mappings[tablename].append(mapping)
		return mappings

	@property
	def data(self):
		return self

	@property
	def construction_kit(self):
		return self.tablenames, self.datatypes	
	
	@staticmethod
	def load_raw_data(raw_data):
		if not raw_data:	
			print("Unexpected error. Pls try again!")
			sys.exit(0)
		data = json.loads(raw_data)
		return data.get('results')		

	@staticmethod
	def load_data_ff(fp):
		with open(fp) as json_file:
			data = json.load(json_file)
			return data.get('results')

	def add_data_ff(self, fp):
		data = DataInterface.load_data_ff(fp)
		Modify.modify_many(data, self)
		return self
	
	def add_from_single_stream(self, packet):
		data = DataInterface.load_raw_data(packet)
		Modify.modify_many(data, self)
		return self

	def add_from_multi_stream(self, stream):
		for packet in stream:	
			data = DataInterface.load_raw_data(packet)
			Modify.modify_many(data, self)
		return self
