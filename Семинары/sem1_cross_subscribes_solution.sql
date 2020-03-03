with orders_with_payments as 
(
select p.amount
, p.payment_id
, oc.order_id
, oc.client_id
, oc.order_date
, row_number() over(partition by payment_id order by order_date) as paymentid_num -- в (2*) смотрите, зачем нам эта строка
from data_task.order_clients oc
left join data_task.payments p 
on oc.client_id = p.client_id -- НО этого не достаточно, так как в (1*) мы выяснили, что client_id могу дублироваться в data_task.order_clients
-- в условие джойна можно писать не только равенство ключей, но и любые булевые условия, например:
and 
(product_type like 'video%' and order_type = 'video' or 
product_type like 'music%' and order_type = 'music') 
)
, 
orders_paymens_clients as 
(
select 
 o.order_id
, o.order_date
, case when paymentid_num = 1 then o.amount end as amount
--все, что не попадет в условие, будет Null, то есть мы таким образом можем "подчистить" дублированные оплаты
, cl.client_id
, cl.age
, cl.geo
, cl.phone_hash
, cl.email_hash
, cl.marketing_channel
from orders_with_payments o 
join data_task.clients cl using(client_id) -- результат будет такой же, как если бы мы написали cl.client_id = o.client_id
)
, 
clients_with_two_orders as --тут САМОЕ интересное, читайте в (3*)
(
select order_id
, lead(order_id) over(partition by client_id order by order_date) as o_id_by_client_id
, lead(order_id) over(partition by phone_hash order by order_date) as o_id_by_phone_hash
, lead(order_id) over(partition by email_hash order by order_date) as o_id_by_email_hash
from orders_paymens_clients
)
--собираем наш источник через 3-2-1:
select 
initial_order.order_id as initial_order_id
, initial_order.order_date as initial_order_date
, initial_order.amount as initial_amount
, initial_order.client_id as initial_client_id
, initial_order.age as initial_client_age
, initial_order.geo as initial_client_geo
, initial_order.marketing_channel as initial_client_marketing_channel
, second_order.order_id  as second_order_id
, second_order.order_date as second_order_date
, second_order.amount as second_amount
, second_order.client_id as second_client_id
, second_order.age as second_client_age
, second_order.marketing_channel as second_client_marketing_channel
from clients_with_two_orders as base_table 
join orders_paymens_clients as initial_order using(order_id)
left join orders_paymens_clients as second_order on 
 second_order.order_id = coalesce(o_id_by_client_id, o_id_by_phone_hash, o_id_by_email_hash)
--вот тут тоже интересно, бегите в (4*) и (5*)
limit 100 --помним, что у нас тут 12К строк в резултате выведутся, вряд ли они все сейчас нужны
--ПРИЛОЖЕНИЕ С КОММЕНТАМИ
--(1*) Как выяснить, есть ли повторы в таблице?
-- select count(client_id) as cnt, count(distinct client_id) as cnt_dist
-- from data_task.order_clients
-- результат:
-- cnt = 12 000	vs cnt_dist = 10 097
-- вывод: есть разные записи с одним и тем же client_id
-- select count(order_id) as cnt, count(distinct order_id) as cnt_dist
-- from data_task.order_clients
-- результат: 
-- cnt = cnt_dist = 12 000
-- вывод: дублирующихся значений order_id нет, можно смело использовать при джойнах
--(2*)
-- -- если мы прогоним вот такую проверку:
-- select payment_id, count(order_id) as cnt
-- from orders_with_payments
-- group by 1
-- having cnt>1
-- -- мы увидим, что задублили порядка 200 оплат. тут дело в изначальных данных, которые сгенерированы случайно
-- -- без учета сложных взаимосвязей. давайте исправим эту ошибку следующим способом: 
-- -- если по нашим условиям сразу несколько order_id будут подходит для нашего payment_id, 
-- -- мы возьмем тот заказ, который был раньше
-- select payment_id, count(order_id) as cnt
-- from orders_with_payments
-- where payment_num = 1
-- group by 1
-- having cnt>1
-- -- здесь в выдаче будет 0 строк, что говорит, что данное условие позволит нам не учитывать задублированные payment_id
--(3*)
--нам нужно по какому-то ключу связать разные order_id, сделанные одним человеком
--по условию задачи для одного client_id может быть несколько order_id, давайте для начала попробуем связаться по нему
--но мы знаем, что такую связь ввели позже. Что делать со старыми order_id, которые заводились до такой функциональности?
--давайте предположим, что один и тот же номер телефона может быть только у одного и того же человека
--то есть phone_hash можем использовать в качестве ключа связи
--аналогично с эл. почтой и email_hash 
-- почему мы подтягиваем только следующий order_id, а не все нужные поля?
-- потому что order_id - первичный ключ и все остальное мы можем подтянуть джойном, не захламляя запрос десятками оконных функций
--(4*)
-- что только что произошло?!?
-- мы создали "словарик" clients_with_two_orders, в котором точно есть все order_id - они будут отвечать на первую подписку
-- и попытки "притянуть" order_id второй подписки по ключам, которые обсудили в (3*)
-- дальше подтягиваем инфаормацию по первам подпискам из orders_paymens_clients - там мы почистили лишние оплаты и добавили инфу по клиенту
-- дальше пытаемся подтягуть информацию по второй подписке тоже из orders_paymens_clients
-- вопрос: законно ли джойнить одну и ту же таблицу 2 раза - более чем! главное, понимать, зачем ты это делаешь))
--(5*)
-- как работает строка second_order.order_d = coalesce(o_id_by_client_id, o_id_by_phone_hash, o_id_by_email_hash)
-- ну вот мы разными способами попытались найти order_id второй покупки
-- мы по какому-то ключу могли найти айди, а по какому-то нет
-- например, человек оставил один и тот же номер, но разные email_hash
-- в таком случае o_id_by_phone_hash будет иметь какон-то значение, а o_id_by_email_hash будет NUll
-- функция coalesce() позволяет выбрать из списка первое не Null значение или отдает Null, если все значения внутри списка Null
-- это значит
-- во-первых, порядок полей важен. 
-- Если мы, например, доверяем order_id, который найден по номеру телефона больше, чем order_id по почте,
-- то сначала должен стоять o_id_by_phone_hash и только потом o_id_by_email_hash
-- во-вторых, если все поля пустые, то наше условие джойна превратится в проверку 
-- " поле с типом integer = null" , результат проверки выдаст нам null, ничего не приджойнится (то, чего мы и добивались))
