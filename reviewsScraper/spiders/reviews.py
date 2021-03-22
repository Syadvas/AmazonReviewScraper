import scrapy
import re
import time
from scrapy import Request
import logging
import json
from bs4 import BeautifulSoup
from lxml import etree
from scrapy.selector import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml.etree import tostring
import pandas as pd




class ReviewsSpider(scrapy.Spider):
    name = 'reviews'
    allowed_domains = ['*']
    #DRIVER_PATH = r"E:\ChromeDriver\chromedriver.exe"
    #driver = webdriver.Chrome(options=options,executable_path=DRIVER_PATH)
    DRIVER_PATH ="/home/shmyadav90s/Downloads/chromedriver"
    driver = webdriver.Chrome(executable_path=DRIVER_PATH)
    df = pd.read_csv('/home/shmyadav90s/Downloads/homeNkitchen_FLL.csv')
    start_urls = list(zip(df.category,df.url))

    
    
    def cleanhtml(self,raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, ' ', raw_html)
        cleantext = [i for i in cleantext.splitlines() if i.strip() !='']
        cleantext = {cleantext[0]:cleantext[1:]}
        return cleantext

    def start_requests(self):
        placeHolder = [] # to check duplicates
        all_products_url = [] # will contain dict with catmap and url of products
        for tup in self.start_urls[:1000]:
            all_products_url_singleCat = [] # will be used as place holder for a single category
            url = tup[1]
            catmap = tup[0]

            self.driver.get(url)
            try:
                for pagesToScrape in range(5): 
                    element = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@class="a-section a-spacing-none a-spacing-top-small"]/h2'))
                    )
                    print('************************************************************')
                    print(pagesToScrape)
                    print('************************************************************')
                    #sleeping so that cooorect html is parsed
                    time.sleep(2)
                    #render html
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")
                    #create xml tree to use xpath
                    dom = etree.HTML(str(soup))
                    #get urls for products
                    products_urls = dom.xpath('//*[@class="a-section a-spacing-none a-spacing-top-small"]/h2/a/@href')
                    for products_url in products_urls:
                        if products_url not in placeHolder:
                            placeHolder.append(products_urls)
                            #update to place holder
                            all_products_url_singleCat =  [products_url] + all_products_url_singleCat
                    #click next page
                    element = self.driver.find_element_by_xpath("//*[@class='a-pagination']//*[contains(text(),'Next')]")
                    element.click()
                #update masterlist
                all_products_url.append({catmap:all_products_url_singleCat})

            except Exception as e:
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                print(e)
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            
        
        with open("urls.json","a") as fl:
            json.dump(all_products_url,fl)
            fl.write('\n')

        
        for al in all_products_url:
            catmap = list(al.keys())[0]
            url_set = al[catmap]
            for url in url_set:

                url = "https://www.amazon.com" + url
                self.driver.get(url)
                #pass
                try:
                    element = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@class="a-size-large product-title-word-break"]'))
                    )
                    #scrolling to element
                    time.sleep(2)
                    element = self.driver.find_element_by_xpath('//*[contains(text(),"Customer reviews")]')
                    self.driver.execute_script("arguments[0].scrollIntoView();", element)
                    
                    #sleeping so that cooorect html is parsed
                    time.sleep(2)
                    #render html
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")
                    #create xml tree to use xpath
                    dom = etree.HTML(str(soup))
                    #get description
                    productDescription = dom.xpath("//*[contains(text(),'Product Description')]//parent::*//p//text()|//*[contains(text(),'Product description')]//parent::*//p//text()")

                    #get title
                    productTitle = dom.xpath('//*[@id="productTitle"]//text()')

                    #get asin
                    asin = dom.xpath("//*[contains(text(),'ASIN')]//following-sibling::span//text()")

                    #ratings
                    ratings = dom.xpath('//*[@class="reviewCountTextLinkedHistogram noUnderline"]/@title')
                    
                    #about Item
                    AboutItem = dom.xpath('//*[@id="feature-bullets"]//li//span//text()')
                    
                    #product info

                    prduct_info_values = dom.xpath('//*[@id="prodDetails"]//table//tr')
                    
                    prduct_info_values = [tostring(i).decode("utf-8").strip() for i in prduct_info_values if 'Customer Reviews' not in tostring(i).decode("utf-8").strip()]
                    prduct_info_values = [self.cleanhtml(i) for i in prduct_info_values]

                    #prduct_info_values = [i.text for i in prduct_info_values if 'Customer Reviews' not in tostring(i).decode("utf-8").strip()]
                    
                    #
                    #product details
                    """
                    We will check for product details only if product info is null.
                    As on page either details or information is present.
                    """
                    productDetails = dom.xpath('//*[@id="detailBulletsWrapper_feature_div"]//text()')
                    #brand
                    ##
                    brand = dom.xpath('//td//*[contains(text(),"Brand")]/parent::td/parent::tr//text()')
                    ##
                    
                    #price
                    price = dom.xpath('//*[@id="priceblock_ourprice"]//text()|//*[@id="priceblock_saleprice"]//text()')
                    #simiral products
                    similarP = dom.xpath('//*[@id="sp_detail"]//*[@class="a-link-normal"]/@title')

                    AboutItem = [i.strip() for i in AboutItem if i.strip()!='']
                    productDetails = [i.strip() for i in productDetails if i.strip()!='']
                    brand = [i.strip() for i in brand if i.strip()!='']

                    SeeAllReviews = dom.xpath('//*[@data-hook="see-all-reviews-link-foot"]//@href')
                    try:
                        SeeAllReviews = "https://www.amazon.com"+ SeeAllReviews[0]
                    except:
                        print('!!!!!!!!!!!!!!!!!!!!!')
                        SeeAllReviews = "None"
                        print('No Reviews')
                        print('!!!!!!!!!!!!!!!!!!!!!')
                    #yield Request(SeeAllReviews,callback=self.parseReviewpage,dont_filter=True)
                    
                    moreAnswerLink = dom.xpath('//*[@class="a-button a-button-base askSeeMoreQuestionsLink"]//@href')
                    try:
                        moreAnswerLink = "https://www.amazon.com"+ moreAnswerLink[0]
                    except:
                        print('!!!!!!!!!!!!!!!!!!!!!')
                        moreAnswerLink = 'None'
                        print('No Questions')
                        print('!!!!!!!!!!!!!!!!!!!!!')
                    #yield Request(moreAnswerLink,callback=self.parseQuestions,dont_filter=True)
                    
                    a = {"productDescription":productDescription,"productTitle":productTitle,"ratings":ratings,"AboutItem":AboutItem,"productDetails":productDetails,"brand":brand,"price": price,'similarProducts':similarP,'product_info':prduct_info_values,"url":url,'catmap':catmap,'moreAnswerLink':moreAnswerLink,'SeeAllReviews':SeeAllReviews}
                    with open("Product.json","a") as fl:
                        json.dump(a,fl)
                        fl.write('\n')

                except Exception as e:
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    
                    print(e)
                    
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

                    yield Request(url,callback=self.parse,dont_filter=True)


    def parseReviewpage(self, response):
        self.driver.get(response.url)
        element = self.driver.find_element_by_xpath("//*[contains(text(),'Top reviews')]")
        element.click()
        time.sleep(2)
        element = self.driver.find_element_by_xpath("//*[contains(text(),'Most recent')]")
        element.click()
        time.sleep(2)
        yield Request(self.driver.current_url,callback=self.parseReview,dont_filter=True)


    def parseReview(self, response):

        reviewTitle = response.xpath('//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-title"]//text()').extract()
        reviewTitle = [i.strip() for i in reviewTitle if i.strip() != '' ]
        reviewRatings = response.xpath('//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-star-rating"]//text()').extract()
        
        reviewText = response.xpath('//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-body"]//text()').extract()
        reviewText = [i.strip() for i in reviewText if i.strip() != '']
        reviewDate = response.xpath('//*[@data-hook="review-date"]/text()').extract()

        all_v = list(zip(reviewTitle,reviewText,reviewRatings,reviewDate))
        for i in all_v:
            rtitle = i[0]
            rtext = i[1]
            rrating = i[2]
            rdate = i[3]
            dict_ = {"asin":self.Asin, "reviewTitle":rtitle,"reviewText":rtext,"reviewRatings":rrating,"reviewDate":rdate}
            with open("Reviews.json","a") as fl:
                json.dump(dict_,fl)
                fl.write('\n')
        nextpage = response.xpath('//*[@class="a-last"]//@href').extract_first()
        print(nextpage)
        nextpage = "https://www.amazon.com" + nextpage
        try:
            yield Request(nextpage,callback=self.parseReview,dont_filter=True)
        except:
            pass


    def parseQuestions(self, response):
        text = response.xpath('//*[@class="a-section askInlineWidget"]//text()').extract()
        #upvotes = response.xpath('//*[@class="a-fixed-left-grid-col a-col-left"]/text()').extract()
        with open(r"questionsAnswers.json",'a') as fl:
            json.dump(text,fl)
            fl.write('\n')
        nextButton = response.xpath('//*[contains(text(),"Next")]//@href').extract_first()
        print("***************************************************************************************")
        print("nextButton")
        if nextButton !="":
            nextButton = "https://www.amazon.com" + nextButton
            yield Request(nextButton,callback=self.parseQuestions,dont_filter=True)
        else:
            pass