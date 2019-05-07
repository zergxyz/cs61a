from urllib.request import urlopen

shake = urlopen('http://composingprograms.com/shakespeare.txt') 
words = set(shake.read().decode().split())
# squares = [n*2 for n in range(3)]
# print(shake.read().decode().split())
print({w for w in words if len(w) == 6 and w[::-1]==w})