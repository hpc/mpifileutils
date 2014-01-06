import subprocess
class job:
	
	id = 0
	
	def __init__(self, id):
		self.id = id

	def state(self):
		dict = {'Q':'queued',
			'R':'running',
			'C':'complete',
			'H':'hold',
			'E':'error'}
		state = subprocess.Popen(['qstat', str(self.id)], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0].split('\n')[2].split()[4]

		return dict[state]

	def id(self):
		return self.id

	def name(self): 
		name = subprocess.Popen(['qstat', str(self.id)], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0].split('\n')[2].split()[1]
		return name

	def user(self):
		user = subprocess.Popen(['qstat', str(self.id)], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0].split('\n')[2].split()[2]
		return user

	def time(self):
		time = subprocess.Popen(['qstat', str(self.id)], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0].split('\n')[2].split()[3]
		return time

	def queue(self):
		queue = subprocess.Popen(['qstat', str(self.id)], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0].split('\n')[2].split()[5]
		return queue

	def delete(self):
		subprocess.call(['qdel', str(self.id)])

	def hold(self):
		subprocess.call(['qhold', str(self.id)])

	def release(self):
		subprocess.call(['qrls', str(self.id)])
		
def submit(job):
	id = subprocess.Popen(['qsub', job], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0]
	return int(id)
def delete(id):
	subprocess.call(['qdel', str(id)])

def hold(id):
	subprocess.call(['qhold', str(id)])

def release(id):
	subprocess.call(['qrls', str(id)])

def getjob(id):
	return job(id)

def getjobsbyuser(user):
	out = subprocess.Popen(['qstat', '-u', user], stderr=subprocess.STDOUT, stdout=subprocess.PIPE).communicate()[0]
	lines = out.split('\n')
	jobdata = lines[5:]
	jobs = []
	print
	for job in jobdata:
		if job:
			jobject = getjob(job.split()[0])
			jobs.append(jobject)
	
	return jobs


