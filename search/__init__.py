import requests


def _main():
    print(search_news())
    pass


def search_news(count=10, verbose=False):
    results = []
    page = 1
    while count > 0:
        if verbose:
            print(f"fetching page {page} with page size {min(100, count)}")
        result = _search_news(page=page, page_size=min(100, count))
        results.extend(result)
        count -= len(result)
        page += 1
    return results


def _search_news(page=1, page_size=25):
    URL = "https://www.newswire.ca/bin/prnj/advSearchCriteriaService"
    payload = {
        "searchPropertyjson": '{"onlyShowMediaTypes":[],"keywordsAny":[],"keywordsAll":[],"keywordsNone":[],"keywordsExact":[],"anyPhrase":{},"companyName":{},"datelineCity":{},"tickerSymbols":{},"industries":[{"label":"Mining/Metals","value":"134"}],"subjects":[],"locations":[],"languages":{},"dateFrom":"01/23/2024","dateTo":"02/23/2026","images":false,"audio":false,"video":false}',
        "pagePath": "/content/newswire-ca/ca/en/search/news",
        "page": 1,
        "pagesize": 25,
        "advSearchType": "newsSearch",
    }
    header = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.7",
        "content-length": "709",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "csrf-token": "undefined",
        "origin": "https://www.newswire.ca",
        "priority": "u=1, i",
        "referer": "https://www.newswire.ca/search/news/?keyword=scJcr",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Brave";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    payload["page"] = page
    payload["pagesize"] = page_size
    response = requests.post(URL, headers=header, data=payload)
    results = response.json()
    results = results["result"]["release"]["hits"]
    out = []
    for result in results:
        for url in result["_source"]["url"]:
            url = str(url)
            if url.startswith("http"):
                out.append(url)
    return out


if __name__ == "__main__":
    _main()
