import re

class Person:
    def __init__(self, firstname, lastname, sex):
        self.firstname = firstname.strip().title()  # title ข่วยแปลงตัวอักษรตัวแรกให้เป็นตัวใหญ่
        self.lastname = lastname.strip().title()
        self.sex = sex

    def __str__(self):
        return '{} {} , sex: {}'.format(self.firstname, self.lastname, self.sex)

# เรียกจาก class Person
class Developer(Person):
    def __init__(self, dev_id, firstname, lastname, sex):
        super().__init__(firstname, lastname, sex)
        self.dev_id = self.remove_non_digit(dev_id)

    # def __str__(self):
    #     return '{} {}'.format(self.dev_id, super(Developer, self).__str__())

    @staticmethod
    def remove_non_digit(dev_id):
        return re.sub(r'[\D]', '', dev_id)  # เป็นการ replace \D คือ เอาทุกตัวที่ไม่ใช่ตัวเลข ให้แทนที่ด้วย ''

    def email(self):
        return '{}.{}@codium.co'.format(self.firstname, self.lastname[0])


class SeniorDeveloper(Developer):
    def __init__(self, dev_id, firstname, lastname, sex, senior_incentive):
        super().__init__(dev_id, firstname, lastname, sex)
        self.senior_incentive = senior_incentive

    def foo(self, dev_id):
        return self.remove_non_digit(dev_id)  # ทดลองเรียกใช้งาน remove_non_digit ก้อใช้งานได้เช่นกัน เพราะเป็น method ที่อยู่ในคลาสแม่ของมัน


if __name__ == '__main__':
    # d1 = Developer('CD-033', 'atthana', 'phiphat', 'male')
    # print(d1)
    # print(d1.email())

    s1 = SeniorDeveloper('CD(001)  ', 'Pawan', 'Nuamjam', 'female', '5000')
    print(s1)
    print(s1.foo('CD(005)'))
    print(s1.email())