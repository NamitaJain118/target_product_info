#####################################################################
# To scrape product info from target.gom
#
# command to run:
#        scrapy crawl project -a url=https://www.target.com/p/-/A-13493042
#        where, 
#           url=https://www.target.com/p/-/A-13493042 
#                                  created by: Namita Jain
#                                  Date: 27-Aug-2023
####################################################################

import scrapy
import json
from pymongo import MongoClient
from re import findall


class MySpider(scrapy.Spider):
    name = "project"  #spider name
    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 5
    }

    def start_requests(self):
        url = getattr(self, 'url', None) #taking an arbitrary single product URL as command line argument
        if url:
            yield scrapy.Request(url, self.page_parse)

    # Uncomment below for insearting scraped data into mongoDB
    # def mongo_connect(self,collection_name):
    #     connection = MongoClient('MONGOURL')
    #     collection = connection['scrape_data'][collection_name]
    #     yield collection

    def page_parse(self,response):
        url=response.xpath('//link[@rel="canonical"]/@href').get()
        raw_data=response.xpath('//script[contains(text(),"__TGT_DATA__")]/text()').get()
        api_key=findall(r'nova\\":{\\"apiKey\\":\\"(.*)\\",\\"baseUrl\\":\\"https://r2d2' ,raw_data)[0]
        all_data=findall(r'deepFreeze\(JSON\.parse\(\"{(.*)\)\), writable:',raw_data)[0].replace("\\","")
        json_raw=findall(r'\"__PRELOADED_QUERIES__\":(.*)',all_data)[0]
        json_data=findall(r'{\"product\":(.*)\}]]',json_raw)[0].replace('u003cBu003e','').replace('u003c/Bu003e','')
        data=json.loads(json_data)
        if data:
            data_list= data.get('item', {}).get('product_description', {}).get('bullet_descriptions')
            product_info = {}
            if data_list:
                for item in data_list:
                    key, value = item.split(":", 1)
                    product_info[key] = value
            else:
                print("features not available on website")
            final_data={
                'url':url,
                'tcin':data.get('tcin'),
                'upc':data.get('item').get('primary_barcode'),
                "price_amount": data.get('price', {}).get('current_retail'),
                "currency": "USD",
                "description": response.xpath('//div[@data-test="item-details-description"]/text()').get(),
                "specs":'',
                "ingredients": data.get('item', {}).get('enrichment', {}).get('nutrition_facts', {}).get('ingredients') if data.get('item').get('enrichment').get('nutrition_facts') else None , 
                "bullets":data.get('item', {}).get('product_description', {}).get('soft_bullet_description'),
                "features": product_info,
            }
            question_data_url=f'https://r2d2.target.com/ggc/Q&A/v1/question-answer?key={api_key}&page=0&questionedId={data.get("tcin")}&type=product&size=100&sortBy=MOST_ANSWERS&errorTag=drax_domain_questions_api_error'
            
            yield scrapy.Request(question_data_url, self.question_parsing,meta=final_data)

    def question_parsing(self,response):
        final_data=response.meta
        data=response.json()
        all_questions=data.get('results')
        question_list=[]
        if all_questions:
            for questions in all_questions:
                answer_list=[]
                if questions.get('answers'):
                    for answers in questions.get('answers'):
                        answer={
                            "answer_id": answers.get('id'),
                            "answer_summary": answers.get('text'),
                            "submission_date": answers.get('submitted_at'),
                            "user_nickname": answers.get('author').get('nickname')
                        }
                        answer_list.append(answer)
                else:
                    print("Answer not available on website")
                data_dict={ 
                        "question_id": questions.get('id'),
                        "submission_date": questions.get("submitted_at"),
                        "question_summary": questions.get('text'),
                        "user_nickname":  questions.get('author').get('nickname'),
                        "answers": answer_list
                }
                question_list.append(data_dict)
        else:
            print("questions not available on website")
        final_data['question'] = question_list
        # TO inseart scraped data into mongoDB
        # data_connection=mongo_connect('product_info')
        # data_connection.insert_one(final_data)

            