# test_with_unittest discover
from fluxer import ordinal_timer_test

def test_ordinal_timer_test():
    time = "05:18:36"
    assert ordinal_timer_test(time) == 0.22125


