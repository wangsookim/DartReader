import os
import io
import json
import requests
import zipfile
import xml.etree.ElementTree as ET
from typing import Optional, List, Union
from datetime import datetime
from dataclasses import dataclass

import pandas as pd
from pandas import json_normalize


@dataclass
class DartBase:
    api_key: str

    @staticmethod
    def request(url: str, params: dict):
        response = requests.get(url, params)

        return response

    @staticmethod
    def load_json(json_response, list_off=False):
        response = json.loads(json_response.text)

        if response['status'] != '000':
            raise ValueError(json_response.text)

        if ('list' not in response) and (list_off is False):
            raise ValueError('list element is not in response message')

        return response

    @staticmethod
    def check_xml(response):

        try:
            tree = ET.XML(response.content)
            status = tree.find('status').text
            message = tree.find('message').text

            if status != '000':
                raise ValueError({'status': status, 'message': message})
        except ET.ParseError as e:
            print(e)

    @staticmethod
    def load_xml(xml):
        zip_file = zipfile.ZipFile(io.BytesIO(xml.content))
        info_list = zip_file.infolist()
        xml_data = zip_file.read(info_list[0].filename)

        try:
            xml_data = xml_data.decode('euc_kr')
        except UnicodeDecodeError as e:
            xml_data = xml_data.decode('utf-8')
        except UnicodeDecodeError as e:
            xml_data = xml_data

        return xml_data

    @staticmethod
    def convert_xml_to_dataframe(xml):
        tree = ET.XML(xml)
        all_records = []

        element = tree.findall('list')
        for i, child in enumerate(element):
            record = {}

            for i, subchild in enumerate(child):
                record[subchild.tag] = subchild.text
            all_records.append(record)

        return pd.DataFrame(all_records)


@dataclass
class DisclosureInfo(DartBase):

    def get_list(self, corp_code: str = '', start: Optional[str] = None, end: Optional[str] = None,
                 kind: str = '', kind_detail: str = '', final: bool = False, paging: bool = False) \
            -> pd.core.frame.DataFrame:
        """공시정보 - 공시검색(목록)

            :param str corp_code: 종목코드
            :param Optional[str] start: 조회 시작일 (default: 1999-01-01)
            :param Optional[str] end: 조회 종료일 (default: today)
            :param str kind: 보고서종류 (A=정기공시, B=주요사항보고, C=발행공시,
                                    D=지분공시, E=기타공시, F=외부감사관련,
                                    G=펀드공시, H=자산유동화, I=거래소공시,
                                    J=공정위공시)
            :param str kind_detail: 공시 상세유형
            :param bool final: 최종보고서 여부 (default: False)
            :param bool paging: response 결과가 여러 page일 때 모든 결과를 가져오고 싶은 경우

            :returns pandas.core.frame.DataFrame: 응답결과
        """

        start = pd.to_datetime(start).strftime('%Y%m%d') if start else ''
        end = pd.to_datetime(end).strftime('%Y%m%d') if end else datetime.today().strftime('%Y%m%d')
        url = 'https://opendart.fss.or.kr/api/list.json'
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bgn_de': start,
            'end_de': end,
            'last_reprt_at': 'Y' if final else 'N',  # 최종보고서 여부
            'page_no': 1,
            'page_count': 100,
        }

        if kind:
            params['pblntf_ty'] = kind  # 공시유형: 기본값 'A'=정기공시
        if kind_detail:
            params['pblnrf_detail'] = kind_detail

        # paging
        while True:
            response = self.request(url, params)
            response = self.load_json(response)

            if params['page_no'] == 1:
                df = json_normalize(response, 'list')
            else:
                df = df.append(json_normalize(response, 'list'))

            if paging is False:
                break

        return df

    def get_company(self, corp_codes: Union[str, List[str]]) -> pd.core.frame.DataFrame:
        """공시정보 - 기업개황
        DART에 등록되어있는 기업의 개황정보를 종목코드로 조회하여 가져옴

        :params str corp_code: 종목코드

        :returns pandas.core.frame.DataFrame: 종목코드에 해당하는 기업의 개황정보
        """
        url = 'https://opendart.fss.or.kr/api/company.json'
        params = {
            'crtfc_key': self.api_key,
            'corp_code': None,
        }

        if type(corp_codes) == str:
            corp_codes = [corp_codes]

        df = pd.DataFrame()
        for corp_code in corp_codes:
            params['corp_code'] = corp_code

            response = self.request(url, params=params)
            response = self.load_json(response, list_off=True)

            temp_df = json_normalize(response)
            temp_df.drop(columns=['status', 'message'], axis=1, inplace=True)
            df = df.append(temp_df)

        return df

    def get_document(self, rcp_no: str) -> str:
        """공시정보 - 공시서류원본
        공시보고서 원본파일을 xml형태로 가져옴

        :param str rcp_no: 공시서류 번호(보고서번호)

        :returns str: 공시서류 원본 xml
        """
        url = 'https://opendart.fss.or.kr/api/document.xml'
        params = {
            'crtfc_key': self.api_key,
            'rcept_no': rcp_no,
        }

        response = self.request(url, params=params)
        self.check_xml(response)
        response = self.load_xml(response)

        return response

    def get_corp_code(self) -> pd.core.frame.DataFrame:
        """공시정보 - 고유번호
        DART에 등록되어있는 공시대상회사의 고유번호, 회사명, 종목코드, 최근변경일자를 가져옴

        :returns pandas.core.frame.DataFrame: DART에 등록되어 있는 공시대상회사 목록
        """
        url = 'https://opendart.fss.or.kr/api/corpCode.xml'
        params = {'crtfc_key': self.api_key, }

        response = self.request(url, params=params)
        self.check_xml(response)
        response = self.load_xml(response)

        response = self.convert_xml_to_dataframe(response)

        return response


@dataclass
class ReportInfo(DartBase):

    def get_report(self, corp_code, keyword, bsns_year, report_code='11011') -> pd.core.frame.DataFrame:
        """사업보고서의 주요정보를 키워드에 따라 가져옴

        :param str corp_code: 고유번호
        :param str keyword: 정보 키워드(증자현황, 배당, 자기주식 취득 및 처분 등등)
        :param int bsns_year: 사업연도
        :param str report_code: 보고서코드(11011: 사업보고서, 11012: 반기보고서, 11013: 1분기보고서, 11014: 3분기보고서)

        :returns pandas.core.frame.DataFrame: 키워드에 해당하는 사업보고서의 주요정보
        """
        keyword_map = {
            '증자': 'irdsSttus',
            '배당': 'alotMatter',
            '자기주식': 'tesstkAcqsDspsSttus',
            '최대주주': 'hyslrSttus',
            '최대주주변동': 'hyslrChgSttus',
            '소액주주': 'mrhlSttus',
            '임원': 'exctvSttus',
            '직원': 'empSttus',
            '임원개인보수': 'hmvAuditIndvdlBySttus',
            '임원전체보수': 'hmvAuditAllSttus',
            '개인별보수': 'indvdlByPay',
            '타법인출자': 'otrCprInvstmntSttus',
        }

        if keyword not in keyword_map.keys():
            raise ValueError(f'keyword is invalid: you can use ont of {keyword_map.keys()}')

        url = f'https://opendart.fss.or.kr/api/{keyword_map[keyword]}.json'
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': report_code, # 보고서 코드 ("11011" = 사업보고서)
        }

        response = self.request(url, params=params)
        response = self.load_json(response, list_off=False)

        df = json_normalize(response, 'list')

        return df


@dataclass
class FinancialInfo(DartBase):

    def __post_init__(self):
        pass