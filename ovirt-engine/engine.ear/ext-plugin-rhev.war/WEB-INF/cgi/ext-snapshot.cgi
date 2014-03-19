#!/usr/bin/python

import cgi
import json
import re
import os
import subprocess as sub

cgiUrl = "/ext-plugin-rhev/cgi/ext-snapshot.cgi"

def executeExtSnapCommand(args=[]):
    #cmd = ['/usr/bin/python',os.path.dirname(__file__)+'/snapshotHandler.py'] + args
    cmd = ['/usr/bin/python',os.path.dirname(__file__)+'/snapHandler.py'] + args
    return sub.Popen(cmd, stdout=sub.PIPE).stdout.read().rstrip('\n')

def getSnapshots(vmname):
    
    lines  = executeExtSnapCommand(['-o','list','-v',vmname]).split('\n')
    for line in lines:
        if line.startswith('['):
            return eval(line)
    return []

def createSelectBox(snapshots, selectedIndex):
    print 'Snapshot:<select name="snapDropDown" onchange="this.form.submit();">'
    for i in range(0, len(snapshots)):
        label = snapshots[i].split('\n')[0]
        selected = 'selected' if selectedIndex == i else ''
        print '<option value="' + str(i) + '" ' + selected + '>' + label + '</option>'
    print '</select>'
    print '<br/><br/>'

def createSnapshotDescription(snapshorts, selectedIndex):
    if selectedIndex == 0:
        return

    printFormattedHTML(snapshorts[selectedIndex])
    print '<br/>'

def createTextArea(selectedIndex):

    cmdArgs = getFormValue('cmdArgs')
    cmdExecuteBtn = getFormValue('cmdExecuteBtn')
    text = cmdArgs if (cmdArgs and cmdExecuteBtn) else ''
    print '<textarea name="cmdArgs" cols="60" rows="2" placeholder="">'
    print text + '</textarea><br/><br/>'
    if selectedIndex == 0 :
        print 'Your New Snapshot Name:<input type="text" name="SnapName" value="" />'
        print '<input type="checkbox" name="withMemory" value="1" checked="checked" /> With memory'
        print '<br/>'
    print '<input type="submit" name="cmdCreateBtn" value="Create"' 
    if selectedIndex:
        print 'disabled="disabled"'
    print '/>'
    #print '<input type="submit" name="cmdListBtn" value="List"' 
    #if selectedIndex:
    #    print 'disabled="disabled"'
    #print '/>'
    print '<input type="submit" name="cmdDeleteBtn" value="Delete"'
    if selectedIndex == 0 :
        print 'disabled="disabled"'
    print '/>'
    print '<input type="submit" name="cmdRestoreBtn" value="Restore"'
    if selectedIndex == 0 :
        print 'disabled="disabled"'
    print '/>'
    print '<br/>'


def createForm(vmname, selectedIndex=0):
    print '<form action="' + cgiUrl + '">'
    print 'VM:<input type="text" name="vmname" value="%s" readonly="readonly" >' % vmname
    snapshots = ['Active VM']
    alist = getSnapshots(vmname)
    if len(alist) > 0:
        snapshots += alist
    createSelectBox(snapshots, selectedIndex)
    createSnapshotDescription(snapshots, selectedIndex)
    createTextArea(selectedIndex)
    print '</form>'

def getFormValue(key):
    form = cgi.FieldStorage()
    return form.getvalue(key)

def printOutput(output):
    try:
        parsedJson = json.loads(output.replace("'", '"'))
        printFormattedHTML(json.dumps(parsedJson, indent=4))
    except:
        printFormattedHTML(output)

def printFormattedHTML(str):
    print str.replace('\n', '<br/>').replace('    ',' ').replace('\t',' ')

def main():
    # Get URL parameters
    form = cgi.FieldStorage()
    vmname = form.getfirst("vmname", "")

    # Print HTML header
    print "Content-Type: text/html\n\n"
    
    action = getFormValue('cmdCreateBtn')
    if action == 'Create':
        #try:
            SnapName = getFormValue('SnapName').strip()
            if len(SnapName) > 0:
                lines  = executeExtSnapCommand(['-o','create','-v',vmname,'-s',SnapName]).split('\n')
                for line in lines:
                    printOutput(line)
                    printOutput('\n')
        #except:
        #    pass

    selectedIndex = getFormValue('snapDropDown')
    if selectedIndex:

        action = getFormValue('cmdDeleteBtn')
        if action == 'Delete':
            alist = getSnapshots(vmname)
            if len(alist) > 0:
                printOutput(alist[int(selectedIndex)-1])
                SnapName = alist[int(selectedIndex)-1]
                if len(SnapName) > 0:
                    lines  = executeExtSnapCommand(['-o','delete','-v',vmname,'-s',SnapName]).split('\n')
                    for line in lines:
                        printOutput(line)
                        printOutput('\n')

        action = getFormValue('cmdRestoreBtn')
        if action == 'Restore':
            alist = getSnapshots(vmname)
            if len(alist) > 0:
                printOutput(alist[int(selectedIndex)-1])
                SnapName = alist[int(selectedIndex)-1]
                if len(SnapName) > 0:
                    lines  = executeExtSnapCommand(['-o','restore','-v',vmname,'-s',SnapName]).split('\n')
                    for line in lines:
                        printOutput(line)
                        printOutput('\n')

        createForm(vmname,int(selectedIndex))
    else:
        createForm(vmname)

    # Execute command
    # output = executeCommand(command)
    # output = command
    # printOutput(output)
    # output = '%s' % executeExtSnapCommand(['-o','list','-v',vmname])
    # printOutput(output)
    # output = '%s' % form
    # printOutput(output)

try:
    main()
except:
    cgi.print_exception()
