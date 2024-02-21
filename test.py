class verify_files:
    
    @staticmethod
    def call_method(full_path):
        print(full_path)

        
class main(verify_files):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.call_func()
        self.call_func_test()
    
    def call_func(self):
        self.call_method(full_path="excel_path")
        
    def call_func_test(self):
        a = verify_files(a=10)

if __name__ == '__main__':
    a = main()