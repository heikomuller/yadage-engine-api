import json
import urllib
import yadage.yadagemodels
import yadage.manualutils

wflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
    json.loads(urllib.urlopen('http://cds-swg1.cims.nyu.edu/yadage-engine/api/v1/workflows/406cd95a-6eb6-4ebe-a0cb-008074114f5f').read())
)

for x in wflow.rules:
    preview = yadage.manualutils.preview_rule(wflow,identifier = x.identifier)
    if not preview: continue
    rule_preview, node_preview = preview
    #print x.identifier
    print str(rule_preview)
    #print node_preview
