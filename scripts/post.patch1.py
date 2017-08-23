import fileinput
import json


for line in fileinput.input():
    post = json.loads(line)
    post.pop('comments', None)

    print(json.dumps(post, ensure_ascii=False, sort_keys=True))

