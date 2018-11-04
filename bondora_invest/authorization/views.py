from django.http import HttpResponse
from django.shortcuts import redirect
from django.http import Http404
import requests
from requests import Request
import boto3
import botocore
import uuid
from django.views.decorators.csrf import csrf_exempt
import json
import psycopg2
from psycopg2 import connect, sql, extras
from django.contrib.auth.decorators import login_required

# Bondora
client_id = "<ID>"
client_secret = "<SECRET>"

# Postgresql on RDS instance
database='bondora_db'
user='ubuntu'
password='<PASSWORD>'
host='<DB_DNS>'


@login_required
def authorization_request(request):
    '''If OK, GET request is redirected by Bondora with code
    in query to redirect url (to function access_token_request).'''
    state = uuid.uuid4().hex
    request.session['bondora_auth_state'] = state
    params = {
        'response_type': 'code',
        'client_id': client_id,
        'state': state,
        'scope': 'ReportCreate'
    }

    bondora_auth_url = 'https://www.bondora.com/oauth/authorize'
    r = Request('GET', url=bondora_auth_url, params=params).prepare()
    return redirect(r.url)


def access_token_request(request):
    '''Upon incoming request graps code from query and
    requests access token from Bondora. 
    Saves acccess_token to database.'''

    original_state = request.session.get('bondora_auth_state')

    if not original_state:
        raise Http404

    del request.session['bondora_auth_state']

    state = request.GET.get('state')
    code = request.GET.get('code')

    if not state or not code:
        raise Http404
    if original_state != state:
        raise Http404

    data = {
        'grant_type': 'authorization_code',
        'client_id': client_id,
        'client_secret': client_secret,
        'code': code
    }

    headers = {'accept': 'application/json'}
    url = "https://api.bondora.com/oauth/access_token"
    r = requests.post(url, data=data, headers=headers)

    if not r.ok:
        raise Http404

    data = r.json()
    access_token = data['access_token']
    
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port='5432')
    cur = conn.cursor() 
    sql = """INSERT INTO authentication (access_token) VALUES(%s);"""
    cur.execute(sql, (access_token,))
    conn.commit()
    cur.close()
    conn.close()

    return HttpResponse(f"Hello. You're at the return page.")


@login_required
def report(request):
    '''Loads access token from database. Sends request for public dataset.
    Saves report in database.'''
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port='5432')
    cur = conn.cursor()

    cur.execute("SELECT access_token FROM authentication ORDER BY id DESC;")
    access_token = cur.fetchone()
    access_token = list(access_token)[0]

    url = 'https://api.bondora.com/api/v1/publicdataset'
    headers = {"Authorization":"Bearer "+access_token, "Accept-Encoding": 'gzip, deflate'}
    params = {
        "Countries":"FI",
        #"LoanDateFrom": "2013-01-01T00:00:00.0000000+03:00",
        "LoanDateFrom": "2015-10-01T00:00:00.0000000+03:00",
        "PageSize":10000,
    }
    r = requests.get(url, params=params, headers=headers)

    if not r.ok:
        raise Http404

    resp_data = r.json()
    resp_data = resp_data['Payload']

    rows_as_tuples = []
    for row in resp_data:
        rows_as_tuples.append(tuple([
        row['ActiveLateCategory'],
        row['ActiveScheduleFirstPaymentReached'],
        row['Age'],
        row['Amount'],
        row['AmountOfPreviousLoansBeforeLoan'],
        row['ApplicationSignedHour'],
        row['ApplicationSignedWeekday'],
        row['AppliedAmount'],
        row['BiddingStartedOn'],
        row['BidsApi'],
        row['BidsManual'],
        row['BidsPortfolioManager'],
        row['City'],
        row['ContractEndDate'],
        row['Country'],
        row['County'],
        row['CreditScoreEeMini'],
        row['CreditScoreEsEquifaxRisk'],
        row['CreditScoreEsMicroL'],
        row['CreditScoreFiAsiakasTietoRiskGrade'],
        row['CurrentDebtDaysPrimary'],
        row['CurrentDebtDaysSecondary'],
        row['DateOfBirth'],
        row['DebtOccuredOn'],
        row['DebtOccuredOnForSecondary'],
        row['DebtToIncome'],
        row['DefaultDate'],
        row['EAD1'],
        row['EAD2'],
        row['EL_V0'],
        row['EL_V1'],
        row['EL_V2'],
        row['Education'],
        row['EmploymentDurationCurrentEmployer'],
        row['EmploymentPosition'],
        row['EmploymentStatus'],
        row['ExistingLiabilities'],
        row['ExpectedLoss'],
        row['ExpectedReturn'],
        row['FirstPaymentDate'],
        row['FreeCash'],
        row['Gender'],
        row['GracePeriodEnd'],
        row['GracePeriodStart'],
        row['HomeOwnershipType'],
        row['IncomeFromChildSupport'],
        row['IncomeFromFamilyAllowance'],
        row['IncomeFromLeavePay'],
        row['IncomeFromPension'],
        row['IncomeFromPrincipalEmployer'],
        row['IncomeFromSocialWelfare'],
        row['IncomeOther'],
        row['IncomeTotal'],
        row['Interest'],
        row['InterestAndPenaltyBalance'],
        row['InterestAndPenaltyDebtServicingCost'],
        row['InterestAndPenaltyPaymentsMade'],
        row['InterestAndPenaltyWriteOffs'],
        row['InterestRecovery'],
        row['LanguageCode'],
        row['LastPaymentOn'],
        row['LiabilitiesTotal'],
        row['ListedOnUTC'],
        row['LoanApplicationStartedDate'],
        row['LoanCancelled'],
        row['LoanDate'],
        row['LoanDuration'],
        row['LoanId'],
        row['LoanNumber'],
        row['LossGivenDefault'],
        row['MaritalStatus'],
        row['MaturityDate_Last'],
        row['MaturityDate_Original'],
        row['ModelVersion'],
        row['MonthlyPayment'],
        row['MonthlyPaymentDay'],
        row['NewCreditCustomer'],
        row['NextPaymentDate'],
        row['NextPaymentNr'],
        row['NoOfPreviousLoansBeforeLoan'],
        row['NrOfDependants'],
        row['NrOfScheduledPayments'],
        row['OccupationArea'],
        row['PlannedInterestPostDefault'],
        row['PlannedInterestTillDate'],
        row['PlannedPrincipalPostDefault'],
        row['PlannedPrincipalTillDate'],
        row['PreviousEarlyRepaymentsBeforeLoan'],
        row['PreviousEarlyRepaymentsCountBeforeLoan'],
        row['PreviousRepaymentsBeforeLoan'],
        row['PrincipalBalance'],
        row['PrincipalDebtServicingCost'],
        row['PrincipalOverdueBySchedule'],
        row['PrincipalPaymentsMade'],
        row['PrincipalRecovery'],
        row['PrincipalWriteOffs'],
        row['ProbabilityOfDefault'],
        row['Rating'],
        row['Rating_V0'],
        row['Rating_V1'],
        row['Rating_V2'],
        row['ReScheduledOn'],
        row['RecoveryStage'],
        row['RefinanceLiabilities'],
        row['Restructured'],
        row['StageActiveSince'],
        row['Status'],
        row['UseOfLoan'],
        row['UserName'],
        row['VerificationType'],
        row['WorkExperience'],
        row['WorseLateCategory']
        ]))

    insert_query = 'insert into public_report values %s'
    psycopg2.extras.execute_values(
    cur, insert_query, rows_as_tuples, template=None, page_size=100)
    conn.commit()
    cur.close()
    conn.close()

    # data = r.json()
    # json_string = json.dumps(data)
    # s3 = boto3.resource('s3')
    # object = s3.Object('b-invest-files', 'report1.txt')
    # object.put(Body=json_string)

    return HttpResponse(f"Hello. You're at the report page.")


# def authorization_request_test(request):
#     state = uuid.uuid4().hex
#     request.session['bondora_auth_state'] = state
#     params = {
#         'response_type': 'code',
#         'client_id': client_id,
#         'state': state,
#         'scope': 'ReportCreate'
#     }

#     url = 'http://127.0.0.1:8000/authorization/process/'
#     r = Request('GET', url=url, params=params).prepare()
#     return redirect(r.url)


# def process_test(request):
#     code = 5
#     url = 'http://127.0.0.1:8000/authorization/end_test/'
#     state = request.GET.get('state')

#     params = {
#         'code': code,
#         'state': state,
#     }

#     r = Request('GET', url=url, params=params).prepare()
#     return redirect(r.url)


# @csrf_exempt
# def access_token_request_test(request):
#     original_state = request.session.get('bondora_auth_state')

#     if not original_state:
#         raise Http404

#     del request.session['bondora_auth_state']

#     state = request.GET.get('state')
#     code = request.GET.get('code')

#     if not state or not code:
#         raise Http404
#     if original_state != state:
#         raise Http404

#     data = {
#         'grant_type': 'authorization_code',
#         'client_id': client_id,
#         'client_secret': client_secret,
#         'code': code
#     }

#     headers = {'accept': 'application/json'}
#     url = 'http://127.0.0.1:8000/authorization/response_test/'
#     r = requests.post(url, data=data, headers=headers)

#     if not r.ok:
#         raise Http404

#     data = r.json()
#     access_token = data['access_token']

#     return HttpResponse(f"Hello. You're at the return page. The access token is: {access_token}")


# @csrf_exempt
# def token_request_respose_test(request):
#     code = request.POST.get("code")
#     if code:
#         data = {"access_token" : "123"}
#         return HttpResponse(json.dumps(data), content_type="application/json")
#     else:
#         return HttpResponse("Hello. You're at at the response page")


# def json_save_test(request):
#     data = {'test':'foo'}
#     json_string = json.dumps(data)
#     s3 = boto3.resource('s3')
#     object = s3.Object('b-invest-files', 'response.txt')
#     object.put(Body=json_string)

#     return HttpResponse(f"Hello. You're at the json save test page.")


# def json_load_test(request):
#     s3 = boto3.resource('s3')
#     object = s3.Object('b-invest-files', 'response.txt')
#     data = object.get()['Body'].read().decode('utf-8')
#     data = json.loads(data)
#     test = data['test']
#     return HttpResponse(f"Hello. You're at the json load test page. Test = {test}")
