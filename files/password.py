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


class LowerCase(PwdChr):
	def __init__(self, char, rank):
		super().__init__(char, rank)


class Digit(PwdChr):
	def __init__(self, char, rank): 					
		super().__init__(char, rank)


class UpperCase(PwdChr):
	def __init__(self, char, rank):
		super().__init__(char, rank)


class SpecialChar(PwdChr):
	def __init__(self, char, rank):
		super().__init__(char, rank)


class PwdCharCreator:
	char_map = {
		**dict.fromkeys(ascii_lowercase, (LowerCase, (b'\x00', 1))),
		**dict.fromkeys(ascii_uppercase, (UpperCase, (b'\x01', 2))),
		**dict.fromkeys(punctuation, (SpecialChar, (b'\x10', 3))),
		**dict.fromkeys(digits, (Digit, (b'\x11', 1)))
		}

	@staticmethod
	def create(chars):
		for char in chars:
			char_obj, rank = PwdCharCreator.char_map[char]
			yield char_obj(char, rank)


class Pwd:
	def __init__(self, pwd):
		self._pwd = [char for char in PwdCharCreator.create(pwd)]
		self._strength = self._get_pwd_strength()

	def __repr__(self):
		return "".join(map(str, self.password))
	
	def __gt__(self, other):
		return self.pwd_strength > other.pwd_strength

	def __lt__(self, other):
		return self.pwd_strength < other.pwd_strength
	
	def __ge__(self, other):
		return self.pwd_strength >= other.pwd_strength

	def __le__(self, other):
		return self.pwd_strength <= other.pwd_strength
	
	@property
	def password(self):
		return self._pwd

	@property
	def pwd_strength(self):
		return self._strength
	
	def _get_pwd_strength(self):
		pwd_index = 0
		if len(self.password) > LENGTH_LIMIT:
			pwd_index += LENGTH_POINTS
		pwd_chars_types = set(getattr(char, 'value') for char in self.password)
		return pwd_index + sum(map(lambda value: value[1], pwd_chars_types))
