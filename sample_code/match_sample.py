name = input("What's your name? ")
"""
showcase of using match in python. Similar to switch in Java
"""
match name: 
    case "Harry" | "Hermione" | "Ron":
        print("Gryffindor")
    case "Draco":
        print("Slytherin")
    case _:
        print("Who?")

# test to remove this comments later