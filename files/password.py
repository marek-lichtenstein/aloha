from string import *

LENGTH_LIMIT = 7
LENGTH_POINTS = 5


class PwdChr:
	def __init__(self, char, rank):
		self._char = char
		self._rank = rank
	
	def __repr__(self):
		return self.char
	
	def __str__(self):
		return self.char

	@property
	def char(self):
		return self._char

	@property
	def value(self):
		return self._rank


class Lowercase(PwdChr):
	def __init__(self, char, rank):
		super().__init__(char, rank)


class Digit(PwdChr):
	def __init__(self, char, rank): 					
		super().__init__(char, rank)


class Uppercase(PwdChr):
	def __init__(self, char, rank):
		super().__init__(char, rank)


class SpecialChar(PwdChr):
	def __init__(self, char, rank):
		super().__init__(char, rank)


class OtherChar(PwdChr):
	def __init__(sefl, char, rank):
		super().__init__(char, rank)


class CharCreator:
	char_map = {
		**dict.fromkeys(ascii_lowercase, (LowerCase, 1)),
		**dict.fromkeys(ascii_uppercase, (UpperCase, 2)),
		**dict.fromkeys(punctuation, (SpecialChar, 3)),
		**dict.fromkeys(digits, (Digit, 1))
		}

	@staticmethod
	def create(chars):
		for char in chars:
			char_obj, rank = CharCreator.char_map.get(char, (OtherChar, 0))
			yield char_obj(char, rank)


class Strength(set):
	def __init__(self):
		super().__init__()
		self.count = 0
	
	def count_char(self, char):
		if type(char) not in self:
			self.add(type(char))
			self.count += char.value
		return char
	
	@property
	def short(self):
		return self.count
	
	@property
	def long(self):
		return self.count + LENGTH_POINTS


class Pwd(Strength):
	def __init__(self, pwd):
		super().__init__()
		self._password = [self.count_char(char) for char in CharCreator.create(pwd)]
		self._strength = self._get_strength()

	def __repr__(self):
		return "".join(map(str, self.password))
	
	def __gt__(self, other):
		return self.strength > other.strength

	def __lt__(self, other):
		return self.strength < other.strength
	
	def __ge__(self, other):
		return self.strength >= other.strength

	def __le__(self, other):
		return self.strength <= other.strength
	
	@property
	def password(self):
		return self._password
	
	@property
	def strength(self):
		return self._strength
	
	def _get_strength(self):
		return self.long if len(self.password) > LENGTH_LIMIT else self.short
