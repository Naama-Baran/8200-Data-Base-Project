from tkinter import filedialog
from functools import partial
from db import *
import webbrowser
from tkinter import *
from PIL import ImageTk, Image
chrome_path = 'open -a /Applications/Google\ Chrome.app %s'
FIELDS=[DBField("link",str),DBField("like",int),DBField("category",str),DBField("comments",str)]
db = DataBase()
table = db.create_table('Pinterest', FIELDS, "link")
table.insert_record({"link":"https://www.pinterest.com/pin/672514156830873075/","like":2,"category":"cookies","comments":"Delicious White, Dark & Milk Chocolate covered pretzel rods."})
table.insert_record({"link":"https://www.pinterest.com/pin/28640147618343746/","like":4,"category":"bedrrom","comments":"i really like there bedding. "})
table.insert_record({"link":"https://www.pinterest.com/pin/262334747032154457/","like":6,"category":"kitchen","comments":"Love the mix of natural woods and painted white woods."})
table.insert_record({"link":"https://www.pinterest.com/pin/466474473906008784/","like":9,"category":"cookies","comments":"Very yummy, but dare I say... 2 cups of chocolate"})
table.insert_record({"link":"https://www.pinterest.com/pin/796011302874835106/","like":32,"category":"animals","comments":"Wow that is sooo cute !!!!! "})

table.insert_record({"link":"https://www.pinterest.com/pin/44754590036277235/","like":32,"category":"food","comments":"Did it without the chocolate and still tasted amazing!"})
table.insert_record({"link":"https://www.pinterest.com/pin/34973334593648799/","like":32,"category":"food","comments":"Chocolate Oreo Truffles "})


table.create_index("comments",'text_index')
# webbrowser.get("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe %s").open("https://www.pinterest.com/pin/28640147618343746/")


def callback(url):
    webbrowser.open_new(url)
def openfn():
    filename = filedialog.askopenfilename(title='open')
    return filename



class MyApp:
    def __init__(self, root):
        self._root = root
        root.title("pinterest")
        self._base_frame = Frame(root)
        self._base_frame.pack(side=TOP)
        img = Image.open("p2.JPG")
        img = img.resize((100, 50), Image.ANTIALIAS)
        img = ImageTk.PhotoImage(img)
        panel = Label(self._base_frame, image=img)
        panel.image = img
        panel.pack(side=LEFT)
        self._label = Label(self._base_frame, text="Enter Your Text",fg="black" ,bg="grey",width=30)
        self._label.pack()
        self._entry = Entry(self._base_frame,width=30)
        self._entry.pack(side=LEFT)
        self._entry.bind("<Return>", self.run)
        self._search = Button(self._base_frame, text='🔍',fg="black", bd = '2',bg="grey",command=self.run)
        self._search.pack(side=LEFT)
        self._results = Frame(root)
        self._results.pack(side=BOTTOM)


    def fix_string(self, string_to_fix):
        return string_to_fix

    def run(self, event=None):
        for widget in self._results.winfo_children():
            widget.destroy()
        string_to_search = self._entry.get()
        if string_to_search[len(string_to_search) - 1] != '#':
            link_list = table.find_in_text_index("comments", string_to_search)
            self.print_match_completions(link_list)
            self._entry.icursor(len(string_to_search))
        else:
            self._entry.delete(0, len(string_to_search))

    def print_match_completions(self, best_five):
        for i in range(len(best_five)):  # use this as row count
            Button(self._results, text=best_five[i],fg="blue", command=partial(callback, best_five[i])).pack()






root = Tk()
MyApp(root)
root.mainloop()

