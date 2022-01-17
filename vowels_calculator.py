vowels =0
consonants=0
word=input("Enter the word: ").lower().strip()
for letter in word:
    if letter.lower() in 'aeiou':
        vowels = vowels + 1
    elif letter=="":
        pass
    else:
        consonants = consonants + 1
    
print("There are {} vowels in the letter".format(vowels))
print("There are {} consonants in the letter".format(consonants))
