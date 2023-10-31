import streamlit as st
from PIL import Image



sidebar_name = "Credits"


def run():
    
     st.title('Credits')
        
     st.markdown('')
     st.markdown('')
        
     #  The Project
     col1, col2= st.columns([1,3])
        
     col1.image('https://i0.wp.com/datascientest.com/wp-content/uploads/2020/08/new-logo.png?fit=229%2C229&ssl=1', width=150)
     col2.markdown(''' ''')
     col2.markdown(''' ''')
     col2.markdown(''' ''')
        
     col2.markdown('''This project was undertaken as part of the MLOps training program at Datascientest.''')
    
     st.markdown('')
     st.markdown('')
    
     
     # Project members
     col1, col2, col3, col4 = st.columns([1,1.25,1.25,1.25])
     
    
     Nom_1 = "Jonathan Cambon"
     lien_url1 = 'https://www.linkedin.com/in/jonathan-cambon/'
     image_url = "https://cdn.icon-icons.com/icons2/2232/PNG/512/linkedin_logo_icon_134604.png"
     Nom_2 = "Djamel Labdouni"
     lien_url2 = 'https://www.linkedin.com/in/djamel-labdouni-024636251/'
     Nom_3 = "    Willy Maillot"
     lien_url3 = 'https://www.linkedin.com/in/willy-maillot-10a8142a/'
     image_width = 30
     espace = "&nbsp;" * 5 
    
     col1.markdown('**Project members:**')
        
     markdown1 = f'{Nom_1}{espace}<a href="{lien_url1}"><img src="{image_url}" width="{image_width}" alt="LinkedIn Logo"></a>'
     col2.markdown(markdown1, unsafe_allow_html=True)
     markdown2 = f'{Nom_2}{espace}<a href="{lien_url2}"><img src="{image_url}" width="{image_width}" alt="LinkedIn Logo"></a>'
     col3.markdown(markdown2, unsafe_allow_html=True)
    
     espace = "&nbsp;" * 10 
     markdown3 = f'{Nom_3}{espace}<a href="{lien_url3}"><img src="{image_url}" width="{image_width}" alt="LinkedIn Logo"></a>'
     col4.markdown(markdown3, unsafe_allow_html=True)
    
     
     st.markdown('')
     st.markdown('')
        
     # Project mentor   
     col1, col2, col3 = st.columns([1,1,1.25])
        
     col1.markdown('**Project mentor:**')
        
     col2.markdown('Vincent Ly')
     col3.markdown('Alban Thuet')
     
     st.markdown('')
     st.markdown('')
     
    
     # Lien GitHub
     col1, col2, col3 = st.columns([1,1,1.25])
        
     col1.markdown('**Github:**')
        
        
     image_url = "https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png"
     lien_url6 =  "https://github.com/WillyMaillot87/projet_mlops_pompiers/tree/master" 
        
     markdown6 = f'<a href="{lien_url6}"><img src="{image_url}" width="{image_width}" alt="GitHub Logo"></a>'
     col2.markdown(markdown6, unsafe_allow_html=True)
    
    
        
    
   