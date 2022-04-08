# Reference : https://docs.python.org/3/library/xml.etree.elementtree.html
import xml.etree.ElementTree as ET

class XMLParser:
    def __init__(self,fName):
        self.fName = fName
        self.tree = ET.parse(self.fName)
        self.root = self.tree.getroot()

    def getWindowparams(self):
        w = self.root.find('window-config')
        wsize = w.find('window-size').text
        wvel = w.find('window-velocity').text
        wunits = w.find('window-units').text
        return {"window_size":wsize, "window_velocity":wvel, "window_units":wunits}

    def getEntryPoints(self):
        dict = {} #{tableName:[list of field names]}
        for dim in self.root.iter('dim'):
            tableName = dim.get('name')
            lst = []
            for field in dim.findall('field'):
                if field.get('is-EntryPoint')=="true":
                    lst.append(field.find('name').text)
            dict[tableName] = lst
        return dict

if __name__=="__main__":
    myparser = XMLParser("config_v2.xml")
    print(myparser.getWindowparams())
    print(myparser.getEntryPoints())
    # print(myparser.getWindowparams())

