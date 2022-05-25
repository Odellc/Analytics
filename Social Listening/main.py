from logging import exception
from multiprocessing.sharedctypes import Value
import os
import sys
import json
from datetime import datetime
from web_scraper import LinkedInWebScraper
from data_lake import DataLake
import configparser
import requests

"""
Action Items:
 [ ] 1) Figure out where to store env variables (username, password, config settings, etc.).
    [x] LinkedIn username and password (Have Bill Yetman share to Boyan / Stilian via LastPass)
    [x] How to access vault?
    [ ] Login info for mailgun
    [x] Azure data lake connection string
 [x] 2) Write resulting data to data lake.
 [ ] 3) Email resulting data to Kristen or whoever.
"""

def send_email(config, subject, body_text, path_to_csv, path_to_html):
    requests.post(
        "https://api.mailgun.net/v3/mailgun.gtmhub.com/messages",
        auth=("api", config.get("mailgun", "api-key")),
        data={"from": config.get("mailgun", "email-from"),
                "to": [config.get("mailgun", "email-to")],
                "subject": subject,
                "text": body_text},
        files=[("attachment", open(path_to_csv))("attachment", open(path_to_html))]
                )

def main():
    """Run main program routine"""
    config = configparser.ConfigParser()
    config.read("/secrets/cfg.ini")

    #retrieve the username/password for all users
    user_dict = {}
    count = 1

    # Retrieve secrets.
    while True:
        try:
            name = config.get("linkedin", f"user{count}_name")
            password = config.get("linkedin", f"user{count}_pass")
        except Exception:
            break    

        user_dict[name] = password
        count += 1

    # Retrieve secrets.
    # user1_name = config.get("linkedin", "user1_name")
    # user1_pass = config.get("linkedin", "user1_pass")
    azure_connection_string = config.get("datalake", "datalake_connection_string")
    driver_choice = config.get("scraper", "scraper_driver")

    search_terms = ["okrs", "strategy"]

    # Create a LinkedIn WebScraper.
    li_scraper = LinkedInWebScraper(driver_choice)


    """
    {
        url: {
            data: {
                name: value,
                discription: value,
                post_url: value
            },
            terms: {"okr", "strategy", etc.} <-set,
            profiles: {"user_name"} <- set
        }
    }
    """


    output_data = {}

    for user_name, user_pass in user_dict.items():

        li_scraper.login(user_name, user_pass)

        for term in search_terms:
            data = li_scraper.search(term)

            for post in data:
                if post["post_url"] in output_data.keys():
                    output_data[post["post_url"]]["terms"].add(term)
                    output_data[post["post_url"]]["profiles"].add(user_name)
                else:
                    output_data[post["post_url"]] = {
                        "data" : post,
                        "terms": set(term),
                        "profiles": set(user_name)
                    }

    # TESTING ONLY!
    # Keeps browser window from closing.
    # while input() != "quit":
    #     pass

    li_scraper.close()

    #Check to see if conversion from set to list is necessary for JSON output

    # Save data to json (temp file).
    print("Saving results...")

    temp_path = os.path.join("output", "linked_search_results.json")
    with open(temp_path, "w", encoding="utf-8") as outfile:
        json.dump(output_data, outfile)

    # Prevent the file from saving to data lake until we know how to access other profiles
    sys.exit()

    # Save results to data lake.
    dl = DataLake(azure_connection_string)

    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d")

    output_path = f"Processed/Social Listening - LinkedIn/social_listening_{timestamp}.json"
    dl.upload_file(output_path, temp_path)

    #converting data into csv
    csv_data = []
    csv_headers = ["Author Name", "Description", "Search Term(s)", "LinkedIn Profile(s)", "Post Url"]

    for url_post in output_data.keys():
        row_post = []
        row_post.append(
            output_data[url_post]["data"]["name"], 
            output_data[url_post]["data"]["description"],
            ",".join(output_data[url_post]["terms"]), 
            ",".join(output_data[url_post]["profiles"]),
            output_data[url_post]["data"]["post_url"])
        csv_data.append(row_post)
    
    csv_output_data = pd.DataFrame(csv_data)
    csv_output_data.columns = csv_headers 

    # Email results to Kristin.
    send_email(config, subject, body_text, path_to_csv, path_to_html)

    # Clean up temp file stored locally.


if __name__ == "__main__":
    main()
