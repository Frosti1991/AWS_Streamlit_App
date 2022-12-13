import streamlit as st
import base64

def set_bg_hack(main_bg):
    '''
    A function to unpack an image from root folder and set as bg.
    Returns: The background.
    '''
    image=st.markdown(
        f"""
        <style>
        [data-testid="stAppViewContainer"] > .main {{
            background: url(data:image/jpg;base64,{base64.b64encode(open(main_bg, "rb").read()).decode()});
            background-size: cover;
            background-position: top left;
            background-repeat: no-repeat;
        }}
        [data-testid="stHeader"] {{
            background: rgba(0,0,0,0);
        }}
        .body {{
            text-align: center;
        }}
        [data-testid="stToolbar"] {{
        right: 2rem;
        }}
         </style>
         """,
         unsafe_allow_html=True
     )

    return image