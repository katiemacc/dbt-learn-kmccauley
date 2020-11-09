select orders.order_id, customers.customer_id, orders.order_date, sum(payment.amount)/100 as usd_amt
from {{ref('stg_orders')}} as orders
inner join {{ref('stg_customers')}} as customers
on customers.customer_id = orders.customer_id
inner join raw.stripe.payment as payment
on orders.order_id = payment.orderid
where payment.status = 'success'
group by 1,2,3
order by 1