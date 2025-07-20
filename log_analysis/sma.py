import itertools
from itertools import islice
from collections import deque


#########################################################
def movingaverage(data, subset_size, data_is_list = None,
		avoid_fp_drift = True):
	'''Return the moving averages of the data, with a window size of
	`subset_size`.  `subset_size` must be an integer greater than 0 and
	less than the length of the input data, or a ValueError will be raised.

	`data_is_list` can be used to tune the algorithm for list or iteratable
	as an input.  The default value, `None` will auto-detect this.
	The algorithm used if `data` is a list is almost twice as fast as if
	it is an iteratable.

	`avoid_fp_drift`, if True (the default) sums every sub-set rather than
	keeping a "rolling sum" (which may be subject to floating-point drift).
	While more correct, it is also dramatically slower for subset sizes
	much larger than 20.

	NOTE: You really should consider setting `avoid_fp_drift = False` unless
	you are dealing with very small numbers (say, far smaller than 0.00001)
	or require extreme accuracy at the cost of execution time.  For
	`subset_size` < 20, the performance difference is very small.
	'''
	if subset_size < 1:
		raise ValueError('subset_size must be 1 or larger')

	if data_is_list is None:
		data_is_list = hasattr(data, '__getslice__')

	divisor = float(subset_size)
	if data_is_list:
		#  This only works if we can re-access old elements, but is much faster.
		#  In other words, it can't be just an iterable, it needs to be a list.

		if subset_size > len(data):
			raise ValueError('subset_size must be smaller than data set size')

		if avoid_fp_drift:
			for x in range(subset_size, len(data) + 1):
				yield sum(data[x - subset_size:x]) / divisor
		else:
			cur = sum(data[0:subset_size])
			yield cur / divisor
			for x in range(subset_size, len(data)):
				cur += data[x] - data[x - subset_size]
				yield cur / divisor
	else:
		#  Based on the recipe at:
		#     http://docs.python.org/library/collections.html#deque-recipes
		it = iter(data)
		d = deque(islice(it, subset_size))

		if subset_size > len(d):
			raise ValueError('subset_size must be smaller than data set size')

		if avoid_fp_drift:
			yield sum(d) / divisor
			for elem in it:
				d.popleft()
				d.append(elem)
				yield sum(d) / divisor
		else:
			s = sum(d)
			yield s / divisor
			for elem in it:
				s += elem - d.popleft()
				d.append(elem)
				yield s / divisor