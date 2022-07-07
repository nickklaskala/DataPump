
#appt_match
try:
	from .appointments.appointments import appointments
except:
	print('failed to load appointments')
	pass

#fill
try:
	from .fill.fill import fill
except:
	print('failed to load fill')
	pass

#other modules
try:
	from .other.load_file_stream import load_file_stream
	from .other.load_dataframe import load_dataframe
	from .other.mask_phi import mask_phi
except:
	print('failed to other modules')
	raise


