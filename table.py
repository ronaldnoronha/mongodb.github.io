import seaborn as sns
import imgkit

class Table:
    def __init__(self, df):
        self.df = df


    def color_coding(self, value, num):
        if value < num:
            color = 'red'
        elif value > num:
            color = 'green'
        else:
            color = 'black'

        return 'color: %s' % color

    def getHtml(self):
        cm = sns.light_palette("green", as_cmap=True)
        return (self.df.style.background_gradient(cmap=cm).render())

    def getJPG(self,file):
        html = self.getHtml()
        with open('table.html', 'w') as f:
            f.write(html)
            f.close()
            pass
        imgkit.from_file('table.html',file)
        return


