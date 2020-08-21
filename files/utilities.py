import time
import urllib.request
from tqdm import tqdm



class ProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

def download_url(size, desc="", verbose=True, remaining_attempts=3, err=""):
	url = f"https://randomuser.me/api/?results={size}"
	if not remaining_attempts:
		if hasattr(err, 'getcode') and err.getcode() == 503:
			print (f'Download failed: due to {err.msg}')
			return -1
		raise err
	try:
		if verbose:
			with ProgressBar(unit='B', unit_scale=True,
									 miniters=1, desc=desc ) as t:
				localfile = urllib.request.urlretrieve(url, reporthook=t.update_to)
				with open(localfile[0]) as lf:
					return lf.read()
		else:
			localfile = urllib.request.urlretrieve(url)
			with open(localfile[0]) as lf:
				return lf.read()

	except Exception as err:
		remaining_attempts -= 1
		print(f"Download failed due to: {err.msg}. Next attempt in...")
		for n in range(5, 0, -1):
			print(n)
			time.sleep(1)
		download_url(size, desc=desc, remaining_attempts=remaining_attempts, err=err)
					

def single_download(size=100, verbose=True):
	if size > 5000: 
		raise ValueError("Use download many!")
	return download_url(size, desc=f'Downloading packet', verbose=verbose)

def download_many(size=10000, verbose=True):
	if size<=5000: 
		raise ValueError("Use single download")
	full_dls, partial = compute_downloads(size)
	if full_dls:
		for i, _ in enumerate(tqdm(range(full_dls), desc='Downloading data')):
			time.sleep(1)
			yield download_url(5000, desc=f'Downloading packet of 5000 records', verbose=verbose)
	if partial:
		yield download_url(partial, desc=f'Downloading packet of {partial} records', verbose=verbose)
	return 1

	
def compute_downloads(size):
	return size // 5000, size % 5000


