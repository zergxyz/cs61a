Aspects of a functional abstraction. To master the use of a functional abstraction, it is often useful to consider its three core attributes. The domain of a function is the set of arguments it can take. The range of a function is the set of values it can return. The intent of a function is the relationship it computes between inputs and output (as well as any side effects it might generate). Understanding functional abstractions via their domain, range, and intent is critical to using them correctly in a complex program.

For example, any square function that we use to implement sum_squares should have these attributes:

* The domain is any single real number.
* The range is any non-negative real number.
* The intent is that the output is the square of the input.
These attributes do not specify how the intent is carried out; that detail is abstracted away.

