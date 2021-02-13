import scrapy
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



class ReviewsSpider(scrapy.Spider):
    name = 'reviews'
    allowed_domains = ['*']
    DRIVER_PATH = r"E:\ChromeDriver\chromedriver.exe"
    driver = webdriver.Chrome(executable_path=DRIVER_PATH)
    start_urls = ["https://www.amazon.com/Non-Pull-Waterproof-Harness-PREVENTS-Accidents/dp/B019QAAENC/ref=sr_1_1?dchild=1&keywords=B019QAAENC&qid=1612381328&sr=8-1"]

    def start_requests(self):
        for url in self.start_urls:
            self.driver.get(url)
            #pass
            try:
                element = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//*[@class="a-size-large product-title-word-break"]'))
                )
                #scrolling to element
                time.sleep(3)
                element = self.driver.find_element_by_xpath('//*[contains(text(),"Customer reviews")]')
                self.driver.execute_script("arguments[0].scrollIntoView();", element)
                
                #sleeping so that cooorect html is parsed
                time.sleep(2)
                #render html
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                #create xml tree to use xpath
                dom = etree.HTML(str(soup))
                #get description
                productDescription = dom.xpath('//*[@id="productDescription"]//p//text()')
                #get title
                productTitle = dom.xpath('//*[@id="productTitle"]//text()')
                #get asin
                asin = dom.xpath("//*[contains(text(),'ASIN')]//following-sibling::span//text()")
                #ratings
                ratings = dom.xpath('//*[@class="reviewCountTextLinkedHistogram noUnderline"]/@title')
                #about Item
                AboutItem = dom.xpath('//*[@id="feature-bullets"]//text()')
                #product details
                productDetails = dom.xpath('//*[@id="detailBulletsWrapper_feature_div"]//text()')
                #brand
                brand = dom.xpath('//td//*[contains(text(),"Brand")]/parent::td/parent::tr//text()')
                #price
                price = dom.xpath('//*[@id="priceblock_ourprice"]//text()')
                #simiral products
                similarP = dom.xpath('//*[@id="sp_detail"]//*[@class="a-link-normal"]/@title')

                AboutItem = [i.strip() for i in AboutItem if i.strip()!='']
                productDetails = [i.strip() for i in productDetails if i.strip()!='']
                brand = [i.strip() for i in brand if i.strip()!='']
                a = {asin[0]:{"productDescription":productDescription,"productTitle":productTitle,"ratings":ratings,"AboutItem":AboutItem,"productDetails":productDetails,"brand":brand,"price":price,'similarProducts':similarP}}
                
                self.Asin = asin
                
                with open("Product.json","a") as fl:
                    json.dump(a,fl)
                    fl.write('\n')

                SeeAllReviews = dom.xpath('//*[@data-hook="see-all-reviews-link-foot"]//@href')
                SeeAllReviews = "https://www.amazon.com"+ SeeAllReviews[0]
                yield Request(SeeAllReviews,callback=self.parseReviewpage,dont_filter=True)
                
                #moreAnswerLink = dom.xpath('//*[@class="a-button a-button-base askSeeMoreQuestionsLink"]//@href')
                #moreAnswerLink = "https://www.amazon.com"+ moreAnswerLink[0]
                #yield Request(moreAnswerLink,callback=self.parseQuestions,dont_filter=True)
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