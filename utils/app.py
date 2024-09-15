import streamlit as st
import lyra

thread = lyra.getaThread()

def get_lyra_response(user_input):
    return lyra.messageGPT(user_input, thread_id=thread)['data']

def main():
    st.set_page_config(page_title="Chat with Lyra", page_icon="🤖", layout="centered")
    # Custom CSS
    st.markdown("""
    <style>
    .stApp {
        background-color: #f0f2f6;
    }
    .stTextInput > div > div > input {
        background-color: #ffffff;
    }
    .stMarkdown {
        font-family: 'Arial', sans-serif;
    }
    </style>
    """, unsafe_allow_html=True)

    # App header
    st.title("Talk with Lyra")
    st.markdown("***Databricks Bot***")

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar="🧑‍💻" if message["role"] == "user" else "🤖"):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("What would you like to ask Lyra?"):
        # User message
        with st.chat_message("user", avatar="🧑‍💻"):
            st.markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Lyra's response
        with st.spinner("Lyra is thinking..."):
            lyra_response = get_lyra_response(prompt)
        with st.chat_message("assistant", avatar="🤖"):
            st.write(lyra_response)
        st.session_state.messages.append({"role": "assistant", "content": lyra_response})

    # Add a footer
    st.markdown("---")
    st.markdown("Powered by Lyra AI | Created by Pipeline Pioneers")

if __name__ == "__main__":
    main()
