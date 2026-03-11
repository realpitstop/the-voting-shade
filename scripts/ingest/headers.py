# UserAgent
from UserAgent import UserAgent
# example: UserAgent = {'User-Agent': Product/Version Description (email)'}

# Only UserAgent
headerUSER = {
    **UserAgent
}

# If getting data repeatedly, keep connection alive
headerKEEPALIVE = {
    **UserAgent,
    'Connection': 'keep-alive'
}

# If recieving compressed items
headersENC = {
    **UserAgent,
    'Connection': 'keep-alive'
}

# If getting data repeatedly and also recieving compressed items
headersKEEPALIVE_ENC = {
    **UserAgent,
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

# For SEC, need host and also the compressed reception
headerSEC = {
    **UserAgent,
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}