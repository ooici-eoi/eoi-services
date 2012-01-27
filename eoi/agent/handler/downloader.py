from HTMLParser import HTMLParser
import urllib

class AnchorParser(HTMLParser):

    _link_names = []
    _in_file_link = False
    def handle_starttag(self, tag, attrs):
        if tag == 'img' and not self._in_file_link: #check the associated image first, to make sure it's a file link
            for key, value in attrs:
                if key == 'alt' and value == '[   ]':
                    self._in_file_link = True
        if tag =='a' and self._in_file_link:
            for key, value in attrs:
                if key == 'href':
                    self._link_names.append(value)
            self._in_file_link = False

def download(url):
    """Copy the contents of a file from a given URL
     to a local file.
     """
    import os
    if not os.path.exists(url.split('/')[-1]):
        webFile = urllib.urlopen(url)
        localFile = open(url.split('/')[-1], 'w')
        localFile.write(webFile.read())
        webFile.close()
        localFile.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2:
        try:
            parser = AnchorParser()
            data = urllib.urlopen(sys.argv[1]).read()
            parser.feed(data)
            file_names = parser._link_names
            for file_name in file_names:
                try:
                    print sys.argv[1] + '/' + file_name
                    download(sys.argv[1] + '/' + file_name)
                except IOError:
                    pass #don't do anything, just go to the next file
        except IOError:
            print 'Filename not found.'
    else:
        import os
        print 'usage: %s http://server.com/path/to/filename' % os.path.basename(sys.argv[0])