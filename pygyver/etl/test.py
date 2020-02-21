import os
from pygyver.etl.toolkit import *
os.environ["EMAIL_HASH_SALT"]='1BC'
print(customer_hash("karim.zaoui@made.com"))
print(stringify(None))
print(is_email_address('karim'))
# anonymizer("My name is Andre")
print(get_yesterday_date())
print(date_lister("2020-01-01", "2020-01-05"))
print(validate_date("2020-01-01"))
