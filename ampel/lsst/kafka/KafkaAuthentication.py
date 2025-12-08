from .SASLAuthentication import SASLAuthentication
from .TLSAuthentication import TLSAuthentication

KafkaAuthentication = SASLAuthentication | TLSAuthentication
