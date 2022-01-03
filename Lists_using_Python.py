List=[]

while len(List) <3:
    new_name=input("Enter the name to be added: ").strip().capitalize()
    print(new_name)
    List.append(new_name)
print("List is full. We cannot add more names")
print("These are the available items in lists : {}".format(List))
