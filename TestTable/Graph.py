import matplotlib.pyplot as plt
import numpy as np

class Graph:
    def Bar(label,values,xLabel,yLabel,title):
        # this is for plotting purpose
        index = np.arange(len(label))
        plt.bar(index, values)
        plt.xlabel(xLabel, fontsize=7)
        plt.ylabel(yLabel, fontsize=5)
        plt.xticks(index, label, fontsize=7, rotation=90)
        plt.title(title)
        #plt.show()
        plt.savefig('static/templates/data.png')

    def h_Bar(label,values,xLabel,yLabel,title):
        index = np.arange(len(label))
        plt.barh(index, values, align='center', alpha=0.5)
        plt.yticks(index,label,fontsize=6)
        #plt.ylabel(label,fontsize=7,rotation=0)
        plt.xlabel(xLabel)
        plt.title(title)
        plt.subplots_adjust(left=0.37)
        plt.savefig('static/templates/data.png')
        #plt.show()


    def Pi(labels,size):
        plt.pie(size, labels=labels,
                autopct='%1.1f%%', shadow=True, startangle=140)

        plt.axis('equal')
        plt.show()
        plt.savefig('static/templates/data.png')


"""label = ['Adventure', 'Action', 'Drama', 'Comedy', 'Thriller/Suspense', 'Horror', 'Romantic Comedy', 'Musical',
         'Documentary', 'Black Comedy', 'Western', 'Concert/Performance', 'Multiple Genres', 'Reality']

no_movies = [
    941,
    854,
    4595,
    2125,
    942,
    509,
    548,
    149,
    1952,
    161,
    64,
    61,
    35,
    5
]

Graph.h_Bar(label,no_movies,"m","s","h")"""