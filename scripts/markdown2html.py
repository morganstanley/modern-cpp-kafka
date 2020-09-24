#!/usr/bin/env python3

import sys
import os
import argparse
import time
import re

import markdown

html_template = '''
<!DOCTYPE html>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title>{title}</title>
    <link href="https://stackpath.bootstrapcdn.com/bootswatch/4.4.1/slate/bootstrap.min.css" rel="stylesheet" integrity="sha384-G9YbB4o4U6WS4wCthMOpAeweY4gQJyyx0P3nZbEBHyz+AtNoeasfRChmek1C2iqV" crossorigin="anonymous">
  </head>
  <body>
    <div class="container">
      {html_content}
      <hr/>
      <footer class="text-center text-muted">
        Generated: {date}
      </footer>
      <hr/>
    </div>
  </body>
</html>'''


extensions = ['toc', 'codehilite', 'meta', 'fenced_code', 'tables']
extension_configs = {
    'toc' : [('anchorlink', True)],
    'codehilite' : [],
    'meta' : []
}
md = markdown.Markdown(extensions=extensions, extension_configs=extension_configs)


def convert(infile, outdir):
    md_content_lines = []
    with open (infile, "r") as md_file:
        is_root_entry = ('README.md' in infile)
        link_pattern = '(.*)\[(.*)\]\(doc/(.*)\.md\)(.*)'
        for line in md_file.readlines():
            found_link = re.match(link_pattern, line)
            if found_link:
                md_content_lines.append(found_link.group(1) + '[' + found_link.group(2) + '](' + ('markdown/' if is_root_entry else '') + found_link.group(3) + '.html)' + found_link.group(4))
            else:
                md_content_lines.append(line)
    md_content = '\n'.join(md_content_lines)
    
    title_found = re.compile("# *(.+)").findall(md_content)
    title = '' if not title_found else title_found[0]
  
    html_content = md.convert(md_content)
  
    html_doc = html_template.format(date=time.strftime("%Y. %m. %d"), title=title, html_content=html_content)

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    outfile =  os.path.join(outdir, os.path.splitext(os.path.basename(infile))[0] + '.html')
    with open(outfile, 'w') as html_file:
        html_file.write(html_doc)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--in-files", nargs='+', help='<Required> Markdown files to convert', required=True)
    parser.add_argument("-o", "--out-dir", help='<Required> The directory to output converted html files', required=True)
    args = parser.parse_args()
    
    for in_file in args.in_files:
        convert(in_file, args.out_dir)

if __name__ == "__main__":
  main()
