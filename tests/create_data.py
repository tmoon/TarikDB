import random as rd

rd.seed(42)

# for i in range(1000):
mu = rd.randint(10000,70000)
for i in range(1000):
	print rd.gauss(mu,mu/4)
