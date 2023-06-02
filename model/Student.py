class Student:
    def __init__(self, id, username, age, email):
        self.id = id
        self.username = username
        self.age = age
        self.email = email

    def __repr__(self):
        return f"<Student {self.id}, {self.username}, {self.age}, {self.email}>"