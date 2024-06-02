
"""
Convert pandas with JSON column to plain pandas dataframe
"""
from typing import List, Dict, Union, Optional
import pandas as pd
import json
from batch_framework.etl import ObjProcessor
from collections import Counter
from urllib.parse import urlparse

class LatestTabularize(ObjProcessor):
    @property
    def input_ids(self):
        return ['latest']

    @property
    def output_ids(self):
        return ['latest_package', 'latest_requirement', 'latest_url', 
                'latest_keyword', 'latest_email']

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        infos = []
        reqs = []
        urls = []
        keywords = []
        keyword_counter = Counter()
        emails = []
        for record in inputs[0].to_dict('records'):
            record['latest'] = json.loads(record['latest'])
            info = LatestTabularize.simplify_record(record)
            _reqs = LatestTabularize.simplify_requires_dist(record)
            _urls = LatestTabularize.simplify_project_urls(record)
            _home_page_url = LatestTabularize.simplify_urls('home_page', record)
            _docs_url = LatestTabularize.simplify_urls('docs_url', record)
            _keywords = LatestTabularize.simplify_keywords(record, counter=keyword_counter)
            _author_emails = LatestTabularize.simplify_emails('author', record)
            _maintainer_emails = LatestTabularize.simplify_emails('maintainer', record)
            infos.append(info)
            reqs.extend(_reqs)
            urls.extend(_urls)
            urls.append(_home_page_url)
            urls.append(_docs_url)
            keywords.extend(_keywords)
            emails.extend(_author_emails)
            emails.extend(_maintainer_emails)
        package_df = pd.DataFrame(infos)
        requirement_df = pd.DataFrame(reqs)
        urls_df = pd.DataFrame(urls)
        keywords_df = pd.DataFrame(keywords)
        emails_df = pd.DataFrame(emails)
        assert len(package_df) > 0
        assert len(requirement_df) > 0
        assert len(urls_df) > 0
        assert len(keywords_df) > 0
        assert len(emails_df) > 0
        print('Package Table Size:', len(package_df))
        print('Requirement Table Size:', len(requirement_df))
        print('Url Table Size:', len(urls_df))
        print('Keywords Table Size:', len(keywords_df))
        print('Email Table Size:', len(emails_df))
        return [package_df, requirement_df, urls_df, keywords_df, emails_df]

    @staticmethod
    def simplify_record(
            record: Dict) -> Dict[str, Union[str, int, float, None]]:
        """Simplify the nestest record dictionary

        Args:
            record (Dict): A nested dictionary

        Returns:
            Dict: The simplified dictionary that is not nested
        """
        return {
            'pkg_name': record['name'],
            'name': record['latest']['info']['name'],
            'package_url': record['latest']['info']['package_url'],
            'project_url': record['latest']['info']['project_url'],
            'requires_python': record['latest']['info']['requires_python'],
            'version': record['latest']['info']['version'],
            'keywords': record['latest']['info']['keywords'],
            'author': record['latest']['info']['author'],
            'author_email': record['latest']['info']['author_email'],
            'maintainer': record['latest']['info']['maintainer'],
            'maintainer_email': record['latest']['info']['maintainer_email'],
            'license': record['latest']['info']['license'],
            'docs_url': record['latest']['info']['docs_url'],
            'home_page': record['latest']['info']['home_page'],
            'num_releases': record['latest']['num_releases'],
            'num_requires_dist': record['latest']['num_info_dependencies']
        }
    
    @staticmethod
    def simplify_urls(url_type: str, record: Dict) -> Dict[str, str]:
        """
        Aggregate different type of urls and extract domain name
        and top level domain name
        """
        assert url_type in ['package_url', 'docs_url', 'home_page']
        pkg_name = record['name']
        url = record['latest']['info'][url_type]
        return {
            'pkg_name': pkg_name,
            'url': url,
            'url_type': url_type,
            **LatestTabularize._extract_url_features(url)
        }
    
    @staticmethod
    def simplify_project_urls(
            record: Dict) -> List[Dict[str, str]]:
        """Simply nested componenet - project_urls in record

        Args:
            record (Dict): A nested dictionary

        Returns:
            List[Dict]:  List of the simplified dictionary that is not nested
        """
        pkg_name = record['name']
        if isinstance(record['latest']['info']['project_urls'], dict):
            results = []
            for key, url in record['latest']['info']['project_urls'].items():
                if url is not None:
                    results.append(
                        {
                            'pkg_name': pkg_name,
                            'url': url,
                            'url_type': key,
                            **LatestTabularize._extract_url_features(url)
                        }
                    )
            return results
        else:
            return []

    @staticmethod
    def _extract_url_features(url: str) -> Dict[str, str]:
        """
        Extract domain, top_level_domain, path from url
        """
        parsed = urlparse(url)
        domain = str(parsed.netloc)
        path = str(parsed.path)
        if '.' in domain:
            top_level_domain = domain.split('.')[-1]
        else:
            top_level_domain = None
        return {
            'domain': domain,
            'top_level_domain': top_level_domain,
            'path': path,
        }

    @staticmethod
    def simplify_emails(role: str, record: Dict) -> List[Dict[str, Optional[str]]]:
        """Make email unnested and extract domain name and top level domain name
        Args:
            role (str): author or maintainer
            record (Dict): A nested dictionary
        Returns:
            List[Dict]:  List of the simplified dictionary with email unnested
        """
        person = record['latest']['info'][role]
        person_email = record['latest']['info'][f'{role}_email']
        results = []
        if isinstance(person_email, str):
            for email in person_email.split(','):
                domain = email.split('@')[-1]
                if '.' in domain:
                    top_level_domain = domain.split('.')[-1]
                else:
                    top_level_domain = None
                results.append({
                    'person_name': person,
                    'email_record': person_email,
                    'email': email,
                    'domain': domain,
                    'top_level_domain': top_level_domain,
                    'role': role
                })
        return results

    @staticmethod
    def simplify_keywords(record: Dict, counter: Counter, threshold: int = 5) -> List[Dict[str, str]]:
        """Make keywords unnested
        Args:
            record (Dict): A nested dictionary
        Returns:
            List[Dict]:  List of the simplified dictionary with keywords unnested
        """
        pkg_name = record['name']
        results = []
        keywords = record['latest']['info']['keywords']
        if isinstance(keywords, str):
            keywords = keywords.strip('[]').strip('""')
            if ',' in keywords:
                LatestTabularize._parse_n_insert_keywords(
                    results, counter, pkg_name, keywords, ','
                )
            else:
                LatestTabularize._parse_n_insert_keywords(
                    results, counter, pkg_name, keywords, ' '
                )
        return results

    @staticmethod
    def _parse_n_insert_keywords(results: List[Dict[str, str]], counter: Counter, pkg_name: str, keywords: str, split_mark: str):
        for keyword in keywords.split(split_mark):
            _keyword = keyword.strip().lower().strip('[]').strip('""')
            if _keyword != '':
                counter[_keyword] += 1
                if counter[_keyword] > 300:
                    results.append({
                        'pkg_name': pkg_name,
                        'keyword': _keyword
                    })


    @staticmethod
    def simplify_requires_dist(
            record: Dict) -> List[Dict[str, Union[str, int, float, None]]]:
        """Simply nested componenet - requires_dict in record

        Args:
            record (Dict): A nested dictionary

        Returns:
            List[Dict]:  List of the simplified dictionary that is not nested
        """
        data = record['latest']['requires']
        if data is not None:
            results = []
            for req_name in data:
                if data[req_name] is None:
                    result = {
                        'pkg_name': record['name'],
                        'required_pkg_name': req_name,
                        'num_match_dist': 0,
                        'requirement_string': req_name,
                        'newest_dist': None,
                        'oldest_dist': None
                    }
                else:    
                    num_match_dist = len(data[req_name]['releases'])
                    if num_match_dist:
                        newest_dist = max(data[req_name]['releases'])
                        oldest_dist = min(data[req_name]['releases'])
                    else:
                        newest_dist = None
                        oldest_dist = None
                    result = {
                        'pkg_name': record['name'],
                        'required_pkg_name': req_name,
                        'num_match_dist': num_match_dist,
                        'requirement_string': data[req_name]['requirement'],
                        'newest_dist': newest_dist,
                        'oldest_dist': oldest_dist
                    }
                results.append(result)
            return results
        else:
            return []

    