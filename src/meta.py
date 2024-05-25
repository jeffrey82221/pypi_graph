from .graph import MetaGraph

subgraphs = {
    'has_requirement': ('package', 'requirement'),
    'has_author': ('package', 'author'),
    'has_maintainer': ('package', 'maintainer'),
    'has_license': ('package', 'license'),
    'has_docs_url': ('package', 'docs_url'),
    'has_home_page': ('package', 'home_page'),
    'has_project_url': ('package', 'project_url')
}

metagraph = MetaGraph(
    subgraphs=subgraphs,
    node_grouping={
        'package': ['package', 'requirement'],
        'person': ['author', 'maintainer'],
        'url': ['docs_url', 'home_page', 'project_url']
    },
    node_grouping_sqls={
        'package': """
            DISTINCT ON (t0.node_id)
            t0.node_id,
            COALESCE(t1.name, t2.name) AS name,
            t1.requires_python,
            t1.version,
            t1.keywords,
            t1.num_releases
        """,
        'person': """
            DISTINCT ON (t0.node_id)
            t0.node_id,
            COALESCE(t1.name, t2.name) AS name,
            COALESCE(t1.email, t2.email) AS email
        """,
        "url": """
            t0.node_id,
            COALESCE(t1.url, t2.url, t3.url) AS url
        """
    },
    link_grouping={
        'has_url': ['has_project_url', 'has_docs_url', 'has_home_page']
    },
    link_grouping_sqls={
        'has_url': """
            t0.from_id,
            t0.to_id,
            COALESCE(t1.url_type, t2.url_type, t3.url_type) AS url_type
        """
    },
    input_ids=[
        'latest_package',
        'latest_requirement',
        'latest_url'
    ],
    node_sqls={
        # Main Package Node
        'package': """
        SELECT
            DISTINCT ON (pkg_name)
            CAST(HASH(pkg_name) AS VARCHAR) AS node_id,
            name,
            package_url,
            project_url,
            requires_python,
            version,
            keywords,
            num_releases
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
        WITH count_table AS (
            SELECT
                license,
                count(*) AS count
            FROM latest_package
            GROUP BY license
        )
        SELECT
            DISTINCT ON (license)
            CAST(HASH(license) AS VARCHAR) AS node_id,
            license AS name
        FROM count_table
        WHERE license IS NOT NULL
            AND license <> 'UNKNOWN'
            AND license <> 'LICENSE.txt'
            AND license <> ''
            AND count >= 2
        """,
        # Docs URL Node
        'docs_url': """
        SELECT
            DISTINCT ON (docs_url)
            CAST(HASH(docs_url) AS VARCHAR) AS node_id,
            docs_url AS url
        FROM latest_package
        WHERE docs_url IS NOT NULL
        """,
        # Home Page URL Node
        'home_page': """
        SELECT
            DISTINCT ON (home_page)
            CAST(HASH(home_page) AS VARCHAR) AS node_id,
            home_page AS url
        FROM latest_package
        WHERE home_page IS NOT NULL
        """,
        # Project URL Node
        'project_url': """
        SELECT
            DISTINCT ON (url)
            CAST(HASH(url) AS VARCHAR) AS node_id,
            url
        FROM latest_url
        WHERE url IS NOT NULL
        AND url <> 'UNKNOWN'
        """
    },
    link_sqls={
        # Has Requirement Link
        'has_requirement': """
        SELECT
            DISTINCT ON (pkg_name, required_pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',required_pkg_name)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(required_pkg_name) AS VARCHAR) AS to_id,
            num_match_dist,
            newest_dist,
            oldest_dist,
            requirement_string
        FROM latest_requirement
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
        # Has License Link
        'has_license': """
        WITH
            count_table AS (
                SELECT
                    license,
                    count(*) AS count
                FROM latest_package
                GROUP BY license
            ),
            node_table AS (
                SELECT
                    DISTINCT ON (license)
                    CAST(HASH(license) AS VARCHAR) AS node_id
                FROM count_table
                WHERE license IS NOT NULL
                    AND license <> 'UNKNOWN'
                    AND license <> 'LICENSE.txt'
                    AND license <> ''
                    AND count >= 2
            ),
            link_table AS (
                SELECT
                    DISTINCT ON (license, pkg_name)
                    CAST(HASH(CONCAT(pkg_name,'|',license)) AS VARCHAR) AS link_id,
                    CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
                    CAST(HASH(license) AS VARCHAR) AS to_id,
                FROM latest_package
            )
        SELECT
            link_id,
            from_id,
            to_id
        FROM link_table
        WHERE EXISTS (
            SELECT * FROM node_table
            WHERE node_table.node_id = link_table.to_id
        )
        """,
        # Docs URL Node
        'has_docs_url': """
        SELECT
            DISTINCT ON (docs_url, pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',docs_url)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(docs_url) AS VARCHAR) AS to_id,
            'Documentation' AS url_type
        FROM latest_package
        WHERE docs_url IS NOT NULL
        """,
        # Home Page URL Node
        'has_home_page': """
        SELECT
            DISTINCT ON (home_page, pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',home_page)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(home_page) AS VARCHAR) AS to_id,
            'Homepage' AS url_type
        FROM latest_package
        WHERE home_page IS NOT NULL
        """,
        # Project URL Node
        'has_project_url': """
        SELECT
            DISTINCT ON (url, pkg_name)
            CAST(HASH(CONCAT(pkg_name,'|',url)) AS VARCHAR) AS link_id,
            CAST(HASH(pkg_name) AS VARCHAR) AS from_id,
            CAST(HASH(url) AS VARCHAR) AS to_id,
            url_type
        FROM latest_url
        WHERE url IS NOT NULL
        AND url <> 'UNKNOWN'
        """
    }
)
print(metagraph.link_grouping)
print(metagraph.triplets)