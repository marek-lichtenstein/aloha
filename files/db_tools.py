from datetime import date
import time

from sqlalchemy import create_engine, func, asc, desc, between
from sqlalchemy.orm import backref, relationship
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select 

from tqdm import tqdm

from files.password import Pwd

datatypes_mapping = {
				str: String, 
				int: Integer
				}


class Schema:
	def create_schema(self, tablename, tablenames, datatypes, *args, **kwargs):
		funcmap = [
        			self._make_pk, 
        			self._make_working_columns
        		]

		schema = {}
		for func in funcmap:
			schema.update(func(tablename, tablenames, datatypes))
		return schema

	def _make_pk(self, *args, **kwargs):
		return {'id': Column(Integer, primary_key=True)}
	
	def _make_working_columns(self,  *args, **kwargs):
		tablename, _, datatypes = args
		return {column_name: Column(datatypes_mapping[datatype.datatype]) for column_name, datatype in datatypes[tablename].items()}


class Table(Schema):
	def __init__(self):
		super()

	def make_table(self, base, tablename, tablenames, datatypes):
		table = type(
					tablename.title(), 
					(base,), 
					{**self.create_schema(tablename, tablenames, datatypes), 
					'__tablename__': tablename}
					)
		return table


class Model(Table):
	def __init__(self, tablenames, datatypes, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.base = declarative_base()
		self.tablenames = tablenames
		self.datatypes = datatypes
		self.tables = self._make_tables()		

	def insert_in_bulk(self, mappings):	
		for mapper, mappings in tqdm(self._bulk_map(mappings).items(), desc="Inserting data", unit="tbl"):			
			with Session() as session:
				session.bulk_insert_mappings(mapper, mappings)

	def get_tables(self, *args):
			
		return [self.tables[arg] for arg in args]
	
	def _bulk_map(self, mappings):
		return {self.tables[tablename]: mappings[tablename] for tablename in self.tablenames}

	def _prepare_tables(self):
		return {
				tablename: self.make_table(self.base, tablename, self.tablenames, self.datatypes)
				for tablename in self.tablenames
				}
	
	def _make_tables(self):
		tables = self._prepare_tables()
		self.base.metadata.create_all(bind=Engine.engine)
		return tables

class Engine:

	default_url = 'sqlite:///:memory:'
	engine = create_engine(default_url)

	@classmethod
	def start_engine(cls, url, *args, **kwargs):
		cls.engine = create_engine(url, *args, **kwargs)
	

class Session:
	def __init__(self, *args, **kwargs):
		self._session = sessionmaker(bind=Engine.engine)
		self._sess = None
	
	@property
	def session(self):
		return self._session
	
	@property
	def active_session(self):
		return self._sess
	
	def __enter__(self):
		self._sess = self.session()
		return self.active_session

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.active_session.commit()
		self.active_session.close()


class Queries:
	def __init__(self, model, *args, **kwargs):
		self.model = model
		self.msgs = {
					'avg-age': lambda date, gender: f"The average age of {gender} users is {date}",
					'gender-pct': lambda result, gender: f"{gender.title()}s states about {result:.1f}% of all users.",
					'cities': lambda results, limit: "{limit} most popular cities:\n{results}".format(
																		limit=limit, results=Queries.join_results(results)
																		),
					'pwds': lambda results, limit: "{limit} most popular passwords:\n{pwd_list}".format(
																		limit=limit,
																		pwd_list=Queries.join_results(results)
																		), 
					'dob-range': lambda results, start, end: "Users born between {start} and {end}:\n{usernames}".format(
																		start=start,
																		end=end,
																		usernames="\n".join(result[0] for result in results)
																		),
					'safest': lambda results: "Safest password(s):\n{pwds}".format(
																		pwds=Queries.safest(results)
																		)
																		}




	def execute_one(self, stmt):
		with Engine.engine.connect() as conn:
			return conn.execute(stmt)

	def execute_many(self, *stmts):
		return [self.execute_one(stmt) for stmt in stmts]

	def decimal_to_date_string(func):
		"""Return years, months day string"""
		def _inner(self, *args, **kwargs):
			number, gender = func(self, *args, **kwargs)
			return f"{int(number)} years, {int(number % 1)} months, {int(number % 1 * 12 % 1 * 30)} days", gender
		return _inner

	def time_counter(func):
		def _inner(self, *args, **kwargs):
			start = time.time()
			info = func(self, *args, **kwargs)
			end = time.time()
			return '{info}.\nExecuted in {exec_time:.4f} sec.'.format(info=info, exec_time=end-start)
		return _inner 

	def info_msg(msg):	
		def _info_avg_age(func):
			def _inner(self, *args, **kwargs):
				if args:
					results, *q_args = func(self, *args, **kwargs)
					return self.msgs[msg](results, *q_args)	
				results = func(self, *args, **kwargs)
				return self.msgs[msg](results)
			return _inner
		return _info_avg_age

	@time_counter
	@info_msg('avg-age')
	@decimal_to_date_string
	def avg_age(self, gender):
		dob_tb, gender_tb = self.model.get_tables('dob','gender')
		stmt = select([func.avg(dob_tb.age)]).\
    					where(gender_tb.gender==gender).\
    					where(dob_tb.id==gender_tb.id)				
		return self.execute_one(stmt).first()[0], gender
	
	@time_counter
	@info_msg('gender-pct')	
	def gender_pct(self, gender):
		gender_tbl = self.model.tables['gender']
		total_stmt = select([func.count(gender_tbl.id)])
		gender_count_stmt = select([func.count(gender_tbl.id)]).where(gender_tbl.gender==gender)
		gender_res, total_res = self.execute_many(gender_count_stmt, total_stmt)
		result = (gender_res.first()[0] / total_res.first()[0]) * 100
		return result, gender

	@time_counter	
	@info_msg('cities')
	def most_freq_cities(self, number_of_results):
		location = self.model.tables['location']
		stmt = select([location.city, func.count(location.id).label('count')]).\
					group_by(location.city).\
					order_by(desc('count')).\
					limit(number_of_results)	
		results = self.execute_one(stmt).fetchall()
		return results, number_of_results	
	
	@time_counter
	@info_msg('pwds')
	def most_freq_pwds(self, number_of_results):
		login = self.model.tables['login']
		stmt = select([login.password, func.count(login.id).label('count')]).\
        			group_by(login.password).\
        			order_by(desc('count')).\
					limit(number_of_results)
		results = self.execute_one(stmt).fetchall()
		return results, number_of_results

	@time_counter
	@info_msg('dob-range')
	def range_dob(self, start, end):
		start, end = Queries.convert_to_datetime(start, end)
		dob, login = self.model.get_tables('dob','login')
		stmt = select([login.username, dob.date]).\
									where(dob.id==login.id).\
									where(between(dob.date, start, end)).\
									order_by(asc(dob.date))	
		results = self.execute_one(stmt).fetchall()
		return results, start, end

	@time_counter
	@info_msg('safest')
	def safest_pwd(self):
		login = self.model.tables['login']
		stmt = select([login.password]).distinct()
		results = self.execute_one(stmt).fetchall()
		return results

	@staticmethod
	def only_safest(passwords):
		sorted_pwds = sorted([Pwd(*password) for password in passwords])
		max_val = 0
		safest_pwds = []
		while True:
			pwd = sorted_pwds.pop()
			if pwd.pwd_strength >= max_val:
				max_val = pwd.pwd_strength
				safest_pwds.append(pwd)
				continue
			break	
		return safest_pwds

	@staticmethod
	def convert_to_datetime(*args):	
		return sorted([date.fromisoformat(arg) for arg in args])

	@staticmethod
	def join_results(results):
		return "\n".join(f'{result[0]}({result[1]})' for result in results)
	
	@staticmethod
	def safest(results):
		return "\n".join(f'{pwd}({pwd.pwd_strength})' for pwd in Queries.only_safest(results))


class DbInterface:
	def __init__(self, tablenames, datatypes):
		self.model = Model(tablenames, datatypes)
		self.queries = Queries(self.model)
	
	def insert_bulked_data(self, mappings):
		self.model.insert_in_bulk(mappings)
		
	def avg_age(self, gender):
		return self.queries.avg_age(gender)
	                                                  
	def gender_pct(self, gender):
		return self.queries.gender_pct(gender)
    
	def most_freq_pwds(self, limit):
		return self.queries.most_freq_pwds(limit)

	def most_freq_cities(self, limit):
		return self.queries.most_freq_cities(limit)

	def safest_pwd(self):
		return self.queries.safest_pwd()

	def range_dob(self, start, end):
		return self.queries.range_dob(start, end)
