# SOP - Radiology DICOM Image QC

**Aspects of a functional abstraction**   
To master the use of a functional abstraction, it is often useful to consider its three core attributes. The domain of a function is the set of arguments it can take. The range of a function is the set of values it can return. The intent of a function is the relationship it computes between inputs and output (as well as any side effects it might generate). Understanding functional abstractions via their domain, range, and intent is critical to using them correctly in a complex program.



I am using cloud dataflow and apache beam to develop my data processing pipeline which required to use pydicom lib in my job. I am trying to add the pydicom dependency in the setup.py file here as followed: 
```python
from setuptools import setup, find_packages

setup(
    name="Test",
    version="1.6.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "apache-beam[gcp]==2.59.0",
        "google-cloud-storage==2.18.2"
        "pydicom==3.0.1"
    ],
    author="MCP Data Engineering Team",
    description="MCP In-house Data deidentification package",
)

```
When I build and deploy my package to the GCP dataflow runner it failed and I got the following error message: 

WARNING: Retrying (Retry(total=2, connect=None, read=None, redirect=None, status=None)) after connection broken by 'SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1006)'))': /simple/pydicom/
Could not fetch URL https://pypi.org/simple/pydicom/: There was a problem confirming the ssl certificate: HTTPSConnectionPool(host='pypi.org', port=443): Max retries exceeded with url: /simple/pydicom/ (Caused by SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1006)'))) - skipping
ERROR: Could not find a version that satisfies the requirement pydicom (from Test) (from versions: none)

However, if I switch to a testing program with same code and same logic but instead of using setup.py I am using the following requirements.txt file attached to my deployed dataflow runner it's working well. The detailed requirements.txt is as followed: 

```
--trusted-host pypi.org 
--trusted-host github.com 
pydicom==3.0.1

```

Based on the error message above (SSL: CERTIFICATE_VERIFY_FAILED) I think it's more related to the security and url access restrition. I am asking your help to see if we can resolve this issue within the setup.py package management as that's our standard production development and delopyment method. Do we have a similar way in setup.py like requirments.txt to let the dataflow contain and vm trust the pipy.org and download the pydicom correctly? If not, what other method we can use to resolve this problem and keep using setup.py to build my package? 
