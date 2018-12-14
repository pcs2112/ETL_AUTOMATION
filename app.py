import sys
import importlib


arg_count = len(sys.argv)


def main(args):
	if len(args) == 1:
		print("Please enter a command.")
		exit()

	try:
		module = importlib.import_module('src.commands')
		cmd = getattr(module, args[1])
	except KeyError as e:
		print(f"\"{args[1]}\" is an invalid command.")
		exit()

	if len(args) > 2:
		cmd(*args[2:len(args)])
	else:
		cmd()


if __name__ == '__main__':
	main(sys.argv)
