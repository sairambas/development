questions = ['Why the sun is hot: ','Is the sun really  yellow: ','Is the sun cool then: ']
index=0
for question in questions:
    answer = input(questions[index]).strip().lower()
    while answer != 'just because':
        answer  = input('Why? :').lower().strip()
        index+=1
 print("Oh okay papa")
