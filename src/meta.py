"""
Defining how links and nodes are extracted from tabular data 
and merged into final links and nodes.

TODO:
- [X] Create Domain with Top-level-domain from email and url node
- [ ] Create path for github url
- [ ] Extract github repo & github author node from path
"""
from .graph import MetaGraph

subgraphs = {
    'has_requirement': ('package', 'requirement'),
    'has_author': ('package', 'author'),
    'has_maintainer': ('package', 'maintainer'),
    'has_license': ('package', 'license'),
    'has_url': ('package', 'url'),
    'has_keyword': ('package', 'keyword'),
    'author_has_email': ('author', 'email'),
    'maintainer_has_email': ('maintainer', 'email'),
    'email_hosted_by': ('email', 'email_domain'),
    'url_hosted_by': ('url', 'url_domain'),
    'released_by': ('package', 'github_account'),
    'released_from': ('package', 'github_repo'),
    'owned_by': ('github_repo', 'github_account')
}

metagraph = MetaGraph(
    subgraphs=subgraphs,
    node_grouping={
        'package': ['package', 'requirement'],
        'person': ['author', 'maintainer'],
        'domain': ['email_domain', 'url_domain']
    },
    node_grouping_sqls={
        'package': """
            DISTINCT ON (t0.node_id)
            t0.node_id,
            COALESCE(t1.name, t2.name) AS name,
            t1.package_url,
            t1.requires_python,
            t1.version,
            t1.num_releases
        """,
        'person': """
            DISTINCT ON (t0.node_id)
            t0.node_id,
            COALESCE(t1.name, t2.name) AS name,
            COALESCE(t1.email, t2.email) AS email
        """,
        'domain': """
            DISTINCT ON (t0.node_id)
            t0.node_id,
            COALESCE(t1.domain, t2.domain) AS domain,
            COALESCE(t1.top_level_domain, t2.top_level_domain) AS top_level_domain
        """
    },
    link_grouping={
        'has_email': ['author_has_email', 'maintainer_has_email']
    },
    link_grouping_sqls={
        'has_email': """
            t1.link_id,
            t0.from_id,
            t0.to_id
        """
    },
    input_ids=[
        'latest_package',
        'latest_requirement',
        'latest_url',
        'latest_keyword',
        'latest_email'
    ],
    node_sqls={
        # Main Package Node
        'package': """
        SELECT
            DISTINCT ON (pkg_name)
            CAST(HASH(pkg_name) AS VARCHAR) AS node_id,
            name,
            package_url,
            requires_python,
            version,
            CAST(num_releases AS INT) AS num_releases
        FROM latest_package
        """,
        # Requirement Package Node
        'requirement': """
        SELECT
            DISTINCT ON (required_pkg_name)
            CAST(HASH(required_pkg_name) AS VARCHAR) AS node_id,
            required_pkg_name AS name,
        FROM latest_requirement
        """,
        # Author Person Node
        'author': """
        SELECT
            DISTINCT ON (author, author_email)
            CAST(HASH(CONCAT(author, '|', author_email)) AS VARCHAR) AS node_id,
            author AS name,
            author_email AS email
        FROM latest_package
        WHERE author IS NOT NULL AND author_email IS NOT NULL
        AND author_email <> ''
        """,
        # Maintainer Person Node
        'maintainer': """
        SELECT
            DISTINCT ON (maintainer, maintainer_email)
            CAST(HASH(CONCAT(maintainer, '|', maintainer_email)) AS VARCHAR) AS node_id,
            maintainer AS name,
            maintainer_email AS email
        FROM latest_package
        WHERE maintainer IS NOT NULL AND maintainer_email IS NOT NULL
        AND maintainer_email <> ''
        """,
        # License Node
        'license': """
        SELECT
            DISTINCT ON (license)
            CAST(HASH(license) AS VARCHAR) AS node_id,
            license AS name
        FROM latest_package
        WHERE license IS NOT NULL
            AND license <> 'UNKNOWN'
            AND license <> 'LICENSE.txt'
            AND license <> ''
        """,
        'keyword': """
        SELECT
            DISTINCT ON (keyword)
            CAST(HASH(keyword) AS VARCHAR) AS node_id,
            keyword
        FROM latest_keyword
        WHERE keyword IS NOT NULL
        AND keyword <> ''
        """,
        'email': """
        SELECT 
            DISTINCT ON (email)
            CAST(HASH(email) AS VARCHAR) AS node_id,
            email,
            domain,
            top_level_domain
        FROM latest_email
        WHERE email IS NOT NULL AND email <> ''
        """,
        # Project URL Node
        'url': """
        SELECT
            DISTINCT ON (url)
            CAST(HASH(url) AS VARCHAR) AS node_id,
            url,
            domain,
            top_level_domain,
            path
        FROM latest_url
        WHERE url IS NOT NULL
        AND url <> 'UNKNOWN'
        AND url <> ''
        """,
        'email_domain': """
        SELECT 
            DISTINCT ON (domain)
            CAST(HASH(domain) AS VARCHAR) AS node_id,
            domain,
            top_level_domain
        FROM latest_email
        WHERE domain IS NOT NULL AND domain <> ''
        """,
        'url_domain': """
        SELECT
            DISTINCT ON (domain)
            CAST(HASH(domain) AS VARCHAR) AS node_id,
            domain,
            top_level_domain
        FROM latest_url
        WHERE domain IS NOT NULL
        AND domain <> 'UNKNOWN'
        AND domain <> ''
        """,
        'github_repo': """
        SELECT
            DISTINCT ON (github_repo)
            CAST(HASH(github_repo) AS VARCHAR) AS node_id,
            github_repo
        FROM latest_url
        WHERE github_repo IS NOT NULL
        AND github_repo <> ''
        """,
        'github_account': """
        SELECT
            DISTINCT ON (github_account)
            CAST(HASH(github_account) AS VARCHAR) AS node_id,
            github_account
        FROM latest_url
        WHERE github_account IS NOT NULL
        AND github_account <> ''
        """,
    },
    link_sqls={
        # Has Requirement Link
        'has_requirement': """
        SELECT
            DISTINCT ON (pkg_name, required_pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',required_pkg_name)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(required_pkg_name) AS VARCHAR) AS to_id,
            CAST(num_match_dist AS INT) AS num_match_dist,
            newest_dist,
            oldest_dist,
            requirement_string
        FROM latest_requirement
        """,
        # Has Keyword Link
        'has_keyword': """
        SELECT
            DISTINCT ON (pkg_name, keyword)
            CAST(HASH(CONCAT(pkg_name,'|',keyword)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(keyword) AS VARCHAR) AS to_id,
        FROM latest_keyword
        WHERE keyword IS NOT NULL
        AND keyword <> ''
        """,
        # Has Author Link
        'has_author': """
        SELECT
            DISTINCT ON (author, author_email, pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',CONCAT(author, '|', author_email))) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(CONCAT(author, '|', author_email)) AS VARCHAR) AS to_id
        FROM latest_package
        WHERE author IS NOT NULL AND author_email IS NOT NULL
        AND author_email <> ''
        """,
        # Has Maintainer Link
        'has_maintainer': """
        SELECT
            DISTINCT ON (maintainer, maintainer_email, pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',CONCAT(maintainer, '|', maintainer_email))) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(CONCAT(maintainer, '|', maintainer_email)) AS VARCHAR) AS to_id
        FROM latest_package
        WHERE maintainer IS NOT NULL AND maintainer_email IS NOT NULL
        AND maintainer_email <> ''
        """,
        # Has Email Link
        'author_has_email': """
        SELECT
            DISTINCT ON (person_name, email_record, email)
            CAST(HASH(CONCAT(CONCAT(person_name, '|', email_record),'|', email)) AS VARCHAR) AS link_id,
            CAST(HASH(CONCAT(person_name, '|', email_record)) AS VARCHAR) AS from_id,
            CAST(HASH(email) AS VARCHAR) AS to_id
        FROM latest_email
        WHERE person_name IS NOT NULL AND email_record IS NOT NULL AND email IS NOT NULL
        AND email_record <> ''
        AND email <> ''
        AND role = 'author'
        """,
        'maintainer_has_email': """
        SELECT
            DISTINCT ON (person_name, email_record, email)
            CAST(HASH(CONCAT(CONCAT(person_name, '|', email_record),'|', email)) AS VARCHAR) AS link_id,
            CAST(HASH(CONCAT(person_name, '|', email_record)) AS VARCHAR) AS from_id,
            CAST(HASH(email) AS VARCHAR) AS to_id
        FROM latest_email
        WHERE person_name IS NOT NULL AND email_record IS NOT NULL AND email IS NOT NULL
        AND email_record <> ''
        AND email <> ''
        AND role = 'maintainer'
        """,
        # Has License Link
        'has_license': """
        SELECT
            DISTINCT ON (pkg_name, license)
            CAST(HASH(CONCAT(pkg_name,'|',license)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(license) AS VARCHAR) AS to_id,
        FROM latest_package
        WHERE license IS NOT NULL
            AND license <> 'UNKNOWN'
            AND license <> 'LICENSE.txt'
            AND license <> ''
        """,
        # Package has URL
        'has_url': """
        SELECT
            DISTINCT ON (pkg_name, url)
            CAST(HASH(CONCAT(pkg_name,'|',url)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(url) AS VARCHAR) AS to_id,
            url_type
        FROM latest_url
        WHERE url IS NOT NULL
        AND url <> 'UNKNOWN'
        AND url <> ''
        """,
        # Package coded from a Github Repo
        'released_from': """
        SELECT
            DISTINCT ON (pkg_name, github_repo)
            CAST(HASH(CONCAT(pkg_name,'|',github_repo)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(github_repo) AS VARCHAR) AS to_id
        FROM latest_url
        WHERE github_repo IS NOT NULL
        AND github_repo <> ''
        """,
        # Package coded by a Github Account
        'released_by': """
        SELECT
            DISTINCT ON (pkg_name, github_account)
            CAST(HASH(CONCAT(pkg_name,'|',github_account)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(github_account) AS VARCHAR) AS to_id
        FROM latest_url
        WHERE github_account IS NOT NULL
        AND github_account <> ''
        """,
        # Github Account belong to a Github Account
        'owned_by': """
        SELECT
            DISTINCT ON (github_repo, github_account)
            CAST(HASH(CONCAT(github_repo,'|',github_account)) AS VARCHAR) AS link_id,
            CAST(HASH(github_repo) AS VARCHAR) AS from_id,
            CAST(HASH(github_account) AS VARCHAR) AS to_id
        FROM latest_url
        WHERE github_account IS NOT NULL
        AND github_account <> ''
        AND github_repo IS NOT NULL
        AND github_repo <> ''
        """,
        # URL hosted by Domain Link
        'url_hosted_by': """
        SELECT
            DISTINCT ON (url, domain)
            CAST(HASH(CONCAT(url,'|', domain)) AS VARCHAR) AS link_id,
            CAST(HASH(url) AS VARCHAR) AS from_id,
            CAST(HASH(domain) AS VARCHAR) AS to_id
        FROM latest_url
        WHERE url IS NOT NULL
        AND url <> 'UNKNOWN'
        AND domain IS NOT NULL
        AND domain <> ''
        """,
        # email hosted by Domain Link
        'email_hosted_by': """
        SELECT
            DISTINCT ON (email, domain)
            CAST(HASH(CONCAT(email,'|', domain)) AS VARCHAR) AS link_id,
            CAST(HASH(email) AS VARCHAR) AS from_id,
            CAST(HASH(domain) AS VARCHAR) AS to_id
        FROM latest_email
        WHERE email_record IS NOT NULL AND email IS NOT NULL AND domain IS NOT NULL
        AND email_record <> ''
        AND email <> ''
        AND domain <> ''
        """,

    }
)
print(metagraph.link_grouping)
print(metagraph.triplets)
