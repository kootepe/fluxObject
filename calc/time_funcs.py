def ordinal_timer(time):
    """
    Helper function to calculate ordinal time from HH:MM:SS

    args:
    ---
    time -- numpy.array
        Array of HH:MM:SS string timestamps

    returns:
    ---
    time -- numpy.array
        Array of float timestamps
    """
    h,m,s = map(int, time.split(':'))
    sec = 60
    secondsInDay = 86400
    ordinal_time = (sec*(sec*h)+sec*m+s)/secondsInDay
    return ordinal_time
