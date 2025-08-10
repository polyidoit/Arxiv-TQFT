#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import re
import json
import arxiv
import yaml
import logging
import argparse
import datetime
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

base_url = "https://arxiv.paperswithcode.com/api/v0/papers/"
github_url = "https://api.github.com/search/repositories"
arxiv_url = "https://arxiv.org/"

# Configure requests session with retry strategy
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Add user agent to avoid blocking
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; ArXiv-Daily-Collector/1.0; +https://github.com/your-username/your-repo)'
    })
    return session

def load_config(config_file:str) -> dict:
    '''
    config_file: input config file path
    return: a dict of configuration
    '''
    # make filters pretty
    def pretty_filters(**config) -> dict:
        keywords = dict()
        EXCAPE = '\"'
        QUOTA = '' # NO-USE
        OR = 'OR' # TODO
        def parse_filters(filters:list):
            ret = ''
            for idx in range(0,len(filters)):
                filter = filters[idx]
                if len(filter.split()) > 1:
                    ret += (EXCAPE + filter + EXCAPE)  
                else:
                    ret += (QUOTA + filter + QUOTA)   
                if idx != len(filters) - 1:
                    ret += OR
            return ret
        for k,v in config['keywords'].items():
            keywords[k] = parse_filters(v['filters'])
        return keywords
    with open(config_file,'r') as f:
        config = yaml.load(f,Loader=yaml.FullLoader) 
        config['kv'] = pretty_filters(**config)
        logging.info(f'config = {config}')
    return config 

def get_authors(authors):
    """
    Concatenate all author names with a comma.
    """
    return ", ".join(author.name for author in authors)

def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output    

def get_daily_papers(topic, query="TQFT", max_results=5):
    """
    @param topic: str
    @param query: str
    @return paper_with_code: dict
    """

    content = dict()
    content_to_web = dict()
    
    # Create session for HTTP requests
    session = create_session()

    # Configure arxiv client with delay
    client = arxiv.Client(
        page_size=10,  # Smaller page size
        delay_seconds=3.0,  # 3 second delay between requests
        num_retries=3
    )

    search_engine = arxiv.Search(
        query = query,
        max_results = max_results,
        sort_by = arxiv.SortCriterion.SubmittedDate
    )

    try:
        results = list(client.results(search_engine))
        logging.info(f"Found {len(results)} papers for topic: {topic}")
        
        if not results:
            logging.warning(f"No papers found for query: {query}")
            return {topic: content}, {topic: content_to_web}

    except Exception as e:
        logging.error(f"Error fetching papers for {topic}: {e}")
        return {topic: content}, {topic: content_to_web}

    for result in results:
        try:
            paper_id            = result.get_short_id()
            paper_title         = result.title
            paper_url           = result.entry_id
            code_url            = base_url + paper_id
            paper_abstract      = result.summary.replace("\n"," ")
            paper_authors       = get_authors(result.authors)
            primary_category    = result.primary_category
            publish_time        = result.published.date()
            update_time         = result.updated.date()
            comments            = result.comment

            logging.info(f"Processing: Time = {update_time} title = {paper_title[:50]}...")

            ver_pos = paper_id.find('v')
            if ver_pos == -1:
                paper_key = paper_id
            else:
                paper_key = paper_id[0:ver_pos]    
            paper_url = arxiv_url + 'abs/' + paper_key
            
            # Try to get code URL with better error handling
            repo_url = None
            try:
                # Add delay before paperswithcode request
                time.sleep(1)
                
                response = session.get(code_url, timeout=10)
                
                if response.status_code == 200:
                    r = response.json()
                    if "official" in r and r["official"]:
                        repo_url = r["official"]["url"]
                        logging.info(f"Found code for {paper_key}: {repo_url}")
                elif response.status_code == 404:
                    logging.debug(f"No code found for {paper_key}")
                else:
                    logging.warning(f"paperswithcode returned {response.status_code} for {paper_key}")
                    
            except requests.exceptions.Timeout:
                logging.warning(f"Timeout getting code for {paper_key}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request error for code {paper_key}: {e}")
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON response for {paper_key}")
            except Exception as e:
                logging.error(f"Unexpected error getting code for {paper_key}: {e}")

            # Format content regardless of code availability
            if repo_url is not None:
                content[paper_key] = "|**{}**|**{}**|{}|[{}]({})|**[link]({})**|\n".format(
                       update_time,paper_title,paper_authors,paper_key,paper_url,repo_url)
                content_to_web[paper_key] = "- {}, **{}**, {}, Paper: [{}]({}), Code: **[{}]({})**".format(
                       update_time,paper_title,paper_authors,paper_url,paper_url,repo_url,repo_url)
            else:
                content[paper_key] = "|**{}**|**{}**|{}|[{}]({})|null|\n".format(
                       update_time,paper_title,paper_authors,paper_key,paper_url)
                content_to_web[paper_key] = "- {}, **{}**, {}, Paper: [{}]({})".format(
                       update_time,paper_title,paper_authors,paper_url,paper_url)

            if comments is not None:
                content_to_web[paper_key] += f", {comments}\n"
            else:
                content_to_web[paper_key] += f"\n"

        except Exception as e:
            logging.error(f"Error processing paper {paper_id}: {e}")
            continue

    logging.info(f"Successfully processed {len(content)} papers for {topic}")
    data = {topic:content}
    data_web = {topic:content_to_web}
    return data,data_web 

def update_json_file(filename,data_dict):
    '''
    daily update json file using data_dict
    '''
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)
            
    json_data = m.copy() 
    
    # update papers in each keywords         
    for data in data_dict:
        for keyword in data.keys():
            papers = data[keyword]

            if keyword in json_data.keys():
                json_data[keyword].update(papers)
            else:
                json_data[keyword] = papers

    with open(filename,"w") as f:
        json.dump(json_data,f)
    
def json_to_md(filename,md_filename,
               task = '',
               to_web = True, 
               use_title = True, 
               use_tc = True,
               show_badge = False,
               use_b2t = True):
    """
    @param filename: str
    @param md_filename: str
    @return None
    """
    def pretty_math(s:str) -> str:
        ret = ''
        match = re.search(r"\$.*\$", s)
        if match == None:
            return s
        math_start,math_end = match.span()
        space_trail = space_leading = ''
        if s[:math_start][-1] != ' ' and '*' != s[:math_start][-1]: space_trail = ' ' 
        if s[math_end:][0] != ' ' and '*' != s[math_end:][0]: space_leading = ' ' 
        ret += s[:math_start] 
        ret += f'{space_trail}${match.group()[1:-1].strip()}${space_leading}' 
        ret += s[math_end:]
        return ret
  
    DateNow = datetime.date.today()
    DateNow = str(DateNow)
    DateNow = DateNow.replace('-','.')
    
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            data = {}
        else:
            data = json.loads(content)

    with open(md_filename,"w+") as f:
        pass

    with open(md_filename,"a+") as f:

        if (use_title == True) and (to_web == True):
            f.write("---\n" + "layout: default\n" + "---\n\n")
        
        if show_badge == True:
            f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
            f.write(f"[![Forks][forks-shield]][forks-url]\n")
            f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
            f.write(f"[![Issues][issues-shield]][issues-url]\n\n")    
                
        if use_title == True:
            f.write("> Automatically updated on " + DateNow + "\n")
        else:
            f.write("> Automatically updated on " + DateNow + "\n")

        f.write("\n")

        if use_tc == True:
            f.write("<details>\n")
            f.write("  <summary>Table of Contents</summary>\n")
            f.write("  <ol>\n")
            for keyword in data.keys():
                day_content = data[keyword]
                if not day_content:
                    continue
                kw = keyword.replace(' ','-')      
                f.write(f"    <li><a href=#{kw.lower()}>{keyword}</a></li>\n")
            f.write("  </ol>\n")
            f.write("</details>\n\n")
        
        for keyword in data.keys():
            day_content = data[keyword]
            if not day_content:
                continue

            f.write(f"## {keyword}\n\n")

            if use_title == True :
                if to_web == False:
                    f.write("|Publish Date|Title|Authors|PDF|Code|\n" + "|---|---|---|---|---|\n")
                else:
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|:---------|:-----------------------|:---------|:------|:------|\n")

            day_content = sort_papers(day_content)
        
            for _,v in day_content.items():
                if v is not None:
                    f.write(pretty_math(v)) 

            f.write(f"\n")
            
            if use_b2t:
                top_info = f"#Updated on {DateNow}"
                top_info = top_info.replace(' ','-').replace('.','')
                f.write(f"<p align=right>(<a href={top_info.lower()}>back to top</a>)</p>\n\n")
            
        if show_badge == True:
            f.write((f"[contributors-shield]: https://img.shields.io/github/"
                     f"contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[contributors-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/graphs/contributors\n"))
            f.write((f"[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[forks-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/network/members\n"))
            f.write((f"[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[stars-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/stargazers\n"))
            f.write((f"[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[issues-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/issues\n\n"))
                
    logging.info(f"{task} finished")        

def demo(**config):
    data_collector = []
    data_collector_web= []
    
    keywords = config['kv']
    max_results = config['max_results']
    publish_readme = config['publish_readme']
    publish_gitpage = config['publish_gitpage']
    publish_wechat = config['publish_wechat']
    show_badge = config['show_badge']

    b_update = config['update_paper_links']
    logging.info(f'Update Paper Link = {b_update}')
    
    if config['update_paper_links'] == False:
        logging.info(f"GET daily papers begin")
        for topic, keyword in keywords.items():
            logging.info(f"Keyword: {topic} - Query: {keyword}")
            try:
                data, data_web = get_daily_papers(topic, query=keyword, max_results=max_results)
                data_collector.append(data)
                data_collector_web.append(data_web)
                
                # Add delay between different topics
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Failed to get papers for {topic}: {e}")
                # Continue with empty data for this topic
                data_collector.append({topic: {}})
                data_collector_web.append({topic: {}})
                
            print("\n")
        logging.info(f"GET daily papers end")

    if publish_readme:
        json_file = config['json_readme_path']
        md_file   = config['md_readme_path']
        if config['update_paper_links']:
            # Note: update_paper_links function is referenced but not defined in original code
            # You may need to implement this function
            pass
        else:    
            update_json_file(json_file,data_collector)
        json_to_md(json_file, md_file, task ='Update Readme', show_badge=show_badge)

    if publish_gitpage:
        json_file = config['json_gitpage_path']
        md_file   = config['md_gitpage_path']
        if config['update_paper_links']:
            # Note: update_paper_links function is referenced but not defined in original code
            # You may need to implement this function
            pass
        else:    
            update_json_file(json_file,data_collector)
        json_to_md(json_file, md_file, task ='Update GitPage', to_web=True, use_title=False, show_badge=show_badge, use_tc=False, use_b2t=False)

    if publish_wechat:
        json_file = config['json_wechat_path']
        md_file   = config['md_wechat_path']
        if config['update_paper_links']:
            # Note: update_paper_links function is referenced but not defined in original code
            # You may need to implement this function  
            pass
        else:    
            update_json_file(json_file, data_collector_web)
        json_to_md(json_file, md_file, task ='Update Wechat', to_web=False, use_title=False, show_badge=show_badge)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path', type=str, default='config.yaml', help='configuration file path')
    parser.add_argument('--update_paper_links', default=False, action="store_true", help='whether to update paper links etc.')                        
    args = parser.parse_args()
    config = load_config(args.config_path)
    config = {**config, 'update_paper_links':args.update_paper_links}
    demo(**config)