please consider the following functions: 
``` python
def if_function(condition, true_result, false_result):
    """Return true_result if condition is a true value, and
    false_result otherwise.

    >>> if_function(True, 2, 3)
    2
    >>> if_function(False, 2, 3)
    3
    >>> if_function(3==2, 3+2, 3-2)
    1
    >>> if_function(3>2, 3+2, 3-2)
    5
    """
    if condition:
        return true_result
    else:
        return false_result

```
in the above example the condition, true_result, false_result will always be evaluated once
before we do any condition inside the code blocks. 

Another interesting example as followed:  
``` python 
a=3
while a:
  a-=5
  print(a)

```
the above function is an infinite loop because in python any value != 0 will be evaluated as True condition 
in the while loop. So we will never get out of the loop. 

```python
def ab(c, d):
     if c > 5:
         print(c)
     elif c > 7:
         print(d)
     print('foo')
ab(10, 20) # will print 10 and foo since if and elif are exclusive with each other 

```