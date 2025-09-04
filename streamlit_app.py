# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(f":cup_with_straw: Pending smoothie orders :cup_with_straw:")
st.write(
  """Orders that need to be fulfilled
  """
)



session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).to_pandas()

if not my_dataframe.empty:
    editable_df = st.data_editor(my_dataframe)
    
    
    
    submitted = st.button('Submit')
    if submitted:
        
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
    
        try:
            og_dataset.merge(edited_dataset, (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                         , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                        )
            st.success("Orders updated.", icon='👍')
    
        except:  
            st.write("Somethin went wrong.")
    
else:
    st.success("There are no pending orders")
