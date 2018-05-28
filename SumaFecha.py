from datetime import datetime
from datetime import timedelta

#Add 1 day
print datetime.now() + timedelta(days=1)

#Subtract 60 seconds
a = datetime.now() + timedelta(hours=24)
a = str(a)
print 

#Add 2 years
print datetime.now() + timedelta(days=730)

#Other Parameters you can pass in to timedelta:
# days, seconds, microseconds,
# milliseconds, minutes, hours, weeks

#Pass multiple parameters (1 day and 5 minutes)
print datetime.now() + timedelta(days=1,minutes=5)
