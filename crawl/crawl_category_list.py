import re
import pandas
import requests

start_url = "http://peraturan.go.id/peraturan/direktori.html"
url = start_url
r = requests.get(url=url)
content = r.content.__str__()
raw_list = re.split('\\n', re.sub(
    '(<div class="col-md-4"><li class="list-group-item"><span class="badge badge-primary">\\d+</span><a href=)|"|(</a></li></div>)',
    '', re.sub('(<div class="row">)|(\\n</div>)', '',
               re.sub('(\\\\[rnt])+', ' ', re.sub('</div>', '</div>\n', re.search(
                   '<div class="tab-pane" id="messages2">((\\\\[rnt]|\\s)+)<ul class="list-group">((\\\\[rnt]|\\s)+)(.+)</ul>(((\\\\[rnt]|\\s)+)</div>)+((\\\\[rnt]|\\s)+)</p>',
                   content).group(5)))).strip()))
categories = []
for category in raw_list:
    temp = re.split('>', category)
    temp[0] = 'http://peraturan.go.id' + temp[0]
    temp[1] = temp[1].title() if not re.match('HAM|BMKG|BMN|BUMN|PNPB|APBN', temp[1]) else temp[1]
    categories.append(temp)

category_df = pandas.DataFrame(categories, columns=['url', 'category'])
category_df.to_csv('category_list.csv')
