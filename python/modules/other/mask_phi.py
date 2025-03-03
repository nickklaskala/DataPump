#coding=utf-8
# -----------------------------------------------------------
#	Name   : mask_phi.py
#	Author : Nick Klaskala
#	Purpose:
#	History: 4/15/22 Nick Klaskala refactor masking_pip_csv_file.py to use config file and seed
#
# -----------------------------------------------------------

#import
import random
import re
import datetime
from faker import Faker
import pandas as pd
import io

#helper function
def nvl(n1,n2):
	return n1 if n1 else n2

#helper function
def toList(input):
	if isinstance(input,list):
		return input
	elif isinstance(input,str):
		return [input]
	else:
		return []

#helper function
#takes first line of raw text and finds most common occurance of characters outside of the a-z list below
def getDelimiter(text):
	firstLine=text.splitlines()[0]
	dct={'|':0}
	for char in set(firstLine):
		if char not in ('abcdefghijklmnopqrstuvwxqyzABCDEFGHIJKLMNOPQRSTUVWXQYZ0123456789_- "().[]{}'):
			dct[char]=firstLine.count(char)
	return (max(dct,key=dct.get))


class Alt:
	''''
	class only has a seed for when a seed is specified in config.json
	class seeds every attribute by the original value+salt if no specified seed in config
	class ALWAYS ALWAYS ALWAYS salts any seeding action
	anytime a seed is not present in the configs it will use funtion reSeed before any attribute retrieval else it will use reSeed for all attributes.
	'''

	def __init__(self, salt, seed=''):
		self.seed              = seed if seed else ''
		self.salt              = salt if salt else ''
		self.saltySeed         = self.seed+self.salt
		self.fake              = Faker()

	def setSeed(self,seed):
		self.seed              = seed if seed else ''
		self.saltySeed         = seed+self.salt

	def reSeed(self,seed):
		self.saltySeed= seed+self.salt
		self.fake.seed_instance(self.saltySeed)

	def clearAttr(self):
		self.first_name        = ''
		self.first_name_male   = ''
		self.first_name_female = ''
		self.last_name         = ''
		self.last_first_male   = ''
		self.last_first_female = ''
		self.last_first        = ''
		self.mrn               = ''
		self.address           = ''
		self.street            = ''
		self.city              = ''
		self.state             = ''
		self.zipcode           = ''
		self.phone             = ''
		self.id                = ''
		self.fin               = ''
		self.dob               = ''

	def f_dob(self,sample='1991-05-30',dateFormat=''):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		self.dob = self.fake.date("%Y-%m-%d",datetime.datetime.now()-datetime.timedelta(weeks=520))

		tm = re.search(r" ?[0-9]*:[0-9]*.*", sample)
		tc = tm.group(0) if tm != None else ''

		dc = re.sub(r" ?[0-9]*:[0-9]*.*","",sample)

		if dateFormat == '' or dateFormat==None:
			if '/' in dc:
				dateFormat= '%m/%d/%Y'
			else:
				dateFormat='%Y-%m-%d'

		dob=datetime.datetime.strptime(self.dob,'%Y-%m-%d')
		formattedDob = dob.strftime(dateFormat)
		return formattedDob+tc

	def f_first_name(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		self.first_name=self.fake.first_name()
		return self.first_name

	def f_last_name(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		self.last_name=self.fake.last_name()
		return self.last_name

	def f_first_name_male(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		self.first_name_male=self.fake.first_name_male()
		self.last_first_male=self.last_name+','+self.first_name_male
		return self.first_name_male

	def f_first_name_female(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		self.first_name_female=self.fake.first_name_female()
		self.last_first_female=self.last_name+','+self.first_name_female
		return self.first_name_female


	def f_last_first_female(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		#little complex but proper utilization of cached names if they exist for all scenarios
		if self.last_name and self.first_name_female:
			self.last_first_female=self.last_name+','+self.first_name_female
		elif not self.last_name and self.first_name_female:
			self.last_name=self.f_last_name(sample)
			self.last_first_female=self.last_name+','+self.first_name_female
		elif self.last_name and not self.first_name_female:
			self.first_name_female=self.f_first_name_female(sample)
			self.last_first_female=self.last_name+','+self.first_name_female
		else:
			self.first_name_female=self.f_first_name_female(sample)
			self.last_name=self.f_last_name(sample)
			self.last_first_female=self.last_name+','+self.first_name_female
		return self.last_first_female


	def f_last_first_male(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		#little complex but proper utilization of cached names if they exist for all scenarios
		if self.last_name and self.first_name_male:
			self.last_first_male=self.last_name+','+self.first_name_male
		elif not self.last_name and self.first_name_male:
			self.last_name=self.f_last_name(sample)
			self.last_first_male=self.last_name+','+self.first_name_male
		elif self.last_name and not self.first_name_male:
			self.first_name_male=self.f_first_name_male(sample)
			self.last_first_male=self.last_name+','+self.first_name_male
		else:
			self.first_name_male=self.f_first_name_male(sample)
			self.last_name=self.f_last_name(sample)
			self.last_first_male=self.last_name+','+self.first_name_male
		return self.last_first_male


	def f_last_first(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		#little complex but proper utilization of cached names if they exist for all scenarios
		if self.last_name and self.first_name:
			self.last_first=self.last_name+','+self.first_name
		elif not self.last_name and self.first_name:
			self.last_name=self.f_last_name(sample)
			self.last_first=self.last_name+','+self.first_name
		elif self.last_name and not self.first_name:
			self.first_name=self.f_first_name(sample)
			self.last_first=self.last_name+','+self.first_name
		else:
			self.first_name=self.f_first_name(sample)
			self.last_name=self.f_last_name(sample)
			self.last_first=self.last_name+','+self.first_name
		return self.last_first


	def f_number(self,sample,length,columnSalt):
		#columnSalt allows you to have multiple columns using a N length numerical generator but will output determinalistic number thats always different for each column
		if length==0:
			return ''
		seed=nvl(self.seed,sample)+self.salt+columnSalt #needs salt cause its using the random object which doesnt use our standard reSeed method
		random.seed(seed)
		return str(random.randint(int('1'*length),int('9'*length)))

	def f_address(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		return self.fake.address().replace('\n',' ')

	def f_street(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		return self.fake.street_address().replace('\n',' ')

	def f_city(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		return self.fake.city()

	def f_state(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		return self.fake.state_abbr()

	def f_zipcode(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		return self.fake.postcode()

	def f_phone(self,sample):
		seed=nvl(self.seed,sample)
		self.reSeed(seed)
		return self.fake.phone_number()


#main module
def mask_phi(etl_config):

	'''{
		 "seed":null
		,"salt":null
		,"first_name":null
		,"last_name":null
		,"last_first":null
		,"address":null
		,"street":null
		,"city":null
		,"state":null
		,"zipcode":null
		,"gender":null
		,"phone":null
		,"num":null
		,"num_01_digit":null
		,"num_02_digit":null
		,"num_03_digit":null
		,"num_04_digit":null
		,"num_05_digit":null
		,"num_06_digit":null
		,"num_07_digit":null
		,"num_08_digit":null
		,"num_09_digit":null
		,"num_10_digit":null
		,"num_11_digit":null
		,"num_12_digit":null
		,"num_13_digit":null
		,"num_14_digit":null
		,"num_15_digit":null
		,"num_16_digit":null
		,"num_17_digit":null
		,"num_18_digit":null
		,"num_19_digit":null
		,"num_20_digit":null
		,"dob":null
		,"dob_format":null
	}'''


	#standard variables
	logger    = etl_config['logger']
	env       = etl_config['env']
	partner   = etl_config['partner']
	file_name = etl_config['file_name']
	logger.info('you are running mask_phi()')

	#module specific variables
	config    = etl_config['config_ext'].get('mask_phi_config',{})

	#build dataframe
	df_name     = config.get('df_name','df')
	df          = etl_config.get(df_name)
	return_type ='df'

	delimiter=None
	if df is None:
		return_type='stream'
		file_stream_name = config.get('file_stream_name','file_stream')
		file_stream      = etl_config.get(file_stream_name)
		delimiter=getDelimiter(file_stream.readline())
		file_stream.seek(0)
		df = pd.read_csv(file_stream,sep=delimiter,dtype=object,keep_default_na=False,encoding='utf8')

	debug=config.get('debug',False)

	#configs
	cf_seed         = toList(config.get('seed'        ))
	cf_salt         =        config.get('salt'        ) #meta should never need more than one
	cf_first_name   = toList(config.get('first_name'  ))
	cf_last_name    = toList(config.get('last_name'   ))
	cf_last_first   = toList(config.get('last_first'  ))
	cf_address      = toList(config.get('address'     ))
	cf_street       = toList(config.get('street'      ))
	cf_city         = toList(config.get('city'        ))
	cf_state        = toList(config.get('state'       ))
	cf_zipcode      = toList(config.get('zipcode'     ))
	cf_gender       =        config.get('gender'      ) #meta should never need more than one and it doesnt need to be masked
	cf_phone        = toList(config.get('phone'       ))
	cf_dob          = toList(config.get('dob'         ))
	cf_dob_format   =        config.get('dob_format'  ) #meta should never need more than one
	cf_num          = toList(config.get('num'         ))
	cf_alpha        = toList(config.get('alpha'       ))
	cf_alphanum     = toList(config.get('alphanum'    ))
	cf_num_01_digit = toList(config.get('num_01_digit'))
	cf_num_02_digit = toList(config.get('num_02_digit'))
	cf_num_03_digit = toList(config.get('num_03_digit'))
	cf_num_04_digit = toList(config.get('num_04_digit'))
	cf_num_05_digit = toList(config.get('num_05_digit'))
	cf_num_06_digit = toList(config.get('num_06_digit'))
	cf_num_07_digit = toList(config.get('num_07_digit'))
	cf_num_08_digit = toList(config.get('num_08_digit'))
	cf_num_09_digit = toList(config.get('num_09_digit'))
	cf_num_10_digit = toList(config.get('num_10_digit'))
	cf_num_11_digit = toList(config.get('num_11_digit'))
	cf_num_12_digit = toList(config.get('num_12_digit'))
	cf_num_13_digit = toList(config.get('num_13_digit'))
	cf_num_14_digit = toList(config.get('num_14_digit'))
	cf_num_15_digit = toList(config.get('num_15_digit'))
	cf_num_16_digit = toList(config.get('num_16_digit'))
	cf_num_17_digit = toList(config.get('num_17_digit'))
	cf_num_18_digit = toList(config.get('num_18_digit'))
	cf_num_19_digit = toList(config.get('num_19_digit'))
	cf_num_20_digit = toList(config.get('num_20_digit'))


	#initilize faker class, initial state is never used but salt is a constant
	alt = Alt(salt=cf_salt)

	#debug - places old column in a new column so we can compare the data that changed
	if debug:
		df.insert(0,"seed",'')
		for col in set(cf_seed+cf_first_name+cf_last_name+cf_last_first+cf_address+cf_street+cf_city+cf_state+cf_zipcode+cf_phone+cf_dob+cf_num+cf_num_01_digit+cf_num_02_digit+cf_num_03_digit+cf_num_04_digit+cf_num_05_digit+cf_num_06_digit+cf_num_07_digit+cf_num_08_digit+cf_num_09_digit+cf_num_10_digit+cf_num_11_digit+cf_num_12_digit+cf_num_13_digit+cf_num_14_digit+cf_num_15_digit+cf_num_16_digit+cf_num_17_digit+cf_num_18_digit+cf_num_19_digit+cf_num_20_digit):
			df.insert(df.columns.get_loc(col),'old_'+col,df[col])

	# mask grid
	for index,row in df.iterrows():

		# clear cache and set seed for faker object for next iteration
		alt.clearAttr()
		seed = ''.join([row[col] for col in cf_seed])
		alt.setSeed(seed)

		#debug add old column/seed, it comes in handy
		if debug and alt.seed:
			row["seed"]=alt.saltySeed

		#birth date
		for col in cf_dob:
			row[col]=alt.f_dob(sample=row[col],dateFormat=cf_dob_format)

		#last name
		for col in cf_last_name :
			row[col]=alt.f_last_name(sample=row[col])

		#first name
		for col in cf_first_name:
			if cf_gender and row[cf_gender].lower() in ('m','male'):
				row[col]=alt.f_first_name_male(sample=row[col])
			elif cf_gender and row[cf_gender].lower() in ('f','female'):
				row[col]=alt.f_first_name_female(sample=row[col])
			else:
				row[col]=alt.f_first_name(sample=row[col])

		# last first
		for col in cf_last_first:
			if cf_gender and row[cf_gender].lower() in ('m','male'):
				row[col]=alt.f_last_first_male(sample=row[col])
			elif cf_gender and row[cf_gender].lower() in ('f','female'):
				row[col]=alt.f_last_first_female(sample=row[col])
			else:
				row[col]=alt.f_last_first(sample=row[col])

		# address
		for col in cf_address:
			row[col]=alt.f_address(sample=row[col])

		# street
		for col in cf_street:
			row[col]=alt.f_street(sample=row[col])

		# city
		for col in cf_city:
			row[col]=alt.f_city(sample=row[col])

		# state
		for col in cf_state:
			row[col]=alt.f_state(sample=row[col])

		#zipcode
		for col in cf_zipcode:
			row[col]=alt.f_zipcode(sample=row[col])

		#phone
		for col in cf_phone:
			row[col]=alt.f_phone(sample=row[col])

		#individually seeded numeric(ie return an numerical value with an length equal  to the original value length)
		for col in cf_num:
			row[col]=alt.f_number(sample=row[col],length=len(row[col]),columnSalt='static')

		#Numerics
		#loop through all the different length numerical configs
		numericalConfigs=enumerate(start=1,iterable=[cf_num_01_digit ,cf_num_02_digit ,cf_num_03_digit ,cf_num_04_digit ,cf_num_05_digit ,cf_num_06_digit ,cf_num_07_digit ,cf_num_08_digit ,cf_num_09_digit ,cf_num_10_digit ,cf_num_11_digit ,cf_num_12_digit ,cf_num_13_digit ,cf_num_14_digit ,cf_num_15_digit ,cf_num_16_digit ,cf_num_17_digit ,cf_num_18_digit ,cf_num_19_digit ,cf_num_20_digit])
		for length,cf_num___digit in numericalConfigs:
			for col in cf_num___digit:
				row[col]=alt.f_number(sample=row[col],length=length,columnSalt=col)


	if return_type=='stream':
		new_stream=io.StringIO()
		df.to_csv(new_stream,sep=delimiter,index=False,line_terminator ='\n')
		new_stream.seek(0)
		etl_config['file_stream']=new_stream
