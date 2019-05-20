"""Optional questions for Lab 1"""

# While Loops

def falling(n, k):
    """Compute the falling factorial of n to depth k.

    >>> falling(6, 3)  # 6 * 5 * 4
    120
    >>> falling(4, 0)
    1
    >>> falling(4, 3)  # 4 * 3 * 2
    24
    >>> falling(4, 1)  # 4
    4
    """
    "*** YOUR CODE HERE ***"
    res = 1;
    if k==0:
        return res
    else:
        for i in range(k):
            res*=(n-i)
        return res 

def double_eights(n):
    """Return true if n has two eights in a row.
    >>> double_eights(8)
    False
    >>> double_eights(88)
    True
    >>> double_eights(2882)
    True
    >>> double_eights(880088)
    True
    >>> double_eights(12345)
    False
    >>> double_eights(80808080)
    False
    """
    "*** YOUR CODE HERE ***"
    
    first_8, second_8 = 0,0
    while (n//10)>0: # 2 digits number at least 
        first_8=(n%10==8)
        n=n//10
        second_8 =(n%10==8) 
        if(first_8==1 and second_8==1):
            return True
    return False

        
