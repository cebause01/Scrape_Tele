import asyncio
import time
import json
import re
from datetime import datetime, timezone
import pandas as pd
import io
import streamlit as st
from telethon import TelegramClient

# CSS to style the form, make inputs smaller, and center the buttons
st.markdown("""
    <style>
        .small-input {
            width: 250px;
            margin-bottom: 10px;
        }
        .input-container {
            display: flex;
            flex-direction: column;
            align-items: flex-start;
        }
        .form-container {
            display: flex;
            justify-content: space-between;
            flex-wrap: wrap;
        }
        .center-button {
            display: block;
            margin: 0 auto;
        }
    </style>
""", unsafe_allow_html=True)

# Function to remove invalid XML characters from text
def remove_unsupported_characters(text):
    valid_xml_chars = (
        "[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD"
        "\U00010000-\U0010FFFF]"
    )
    return re.sub(valid_xml_chars, '', text)

# Function to format time in days, hours, minutes, and seconds
def format_time(seconds):
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f'{int(days):02}:{int(hours):02}:{int(minutes):02}:{int(seconds):02}'

# Function to print progress of the scraping process
def print_progress(t_index, message_id, start_time, max_t_index):
    elapsed_time = time.time() - start_time
    current_progress = t_index / (t_index + message_id) if (t_index + message_id) <= max_t_index else t_index / max_t_index
    percentage = current_progress * 100
    estimated_total_time = elapsed_time / current_progress
    remaining_time = estimated_total_time - elapsed_time

    elapsed_time_str = format_time(elapsed_time)
    remaining_time_str = format_time(remaining_time)

    print(f'Progress: {percentage:.2f}% | Elapsed Time: {elapsed_time_str} | Remaining Time: {remaining_time_str}')

# Async function for scraping
async def scrape_telegram(username, phone, api_id, api_hash, channels, date_min, date_max, file_name, key_search, max_t_index, time_limit, file_format):
    data = []
    t_index = 0
    start_time = time.time()

    # Loop through channels
    for channel in channels:
        if t_index >= max_t_index:
            break

        if time.time() - start_time > time_limit:
            break

        loop_start_time = time.time()

        try:
            c_index = 0
            # Asynchronous TelegramClient context
            async with TelegramClient(username, api_id, api_hash) as client:
                async for message in client.iter_messages(channel, search=key_search):
                    try:
                        if date_min <= message.date <= date_max:
                            comments_list = []

                            # Process comments (replies) to the message
                            try:
                                async for comment_message in client.iter_messages(channel, reply_to=message.id):
                                    comment_text = comment_message.text.replace("'", '"')
                                    comment_media = 'True' if comment_message.media else 'False'

                                    comment_emoji_string = ''
                                    if comment_message.reactions:
                                        for reaction_count in comment_message.reactions.results:
                                            emoji = reaction_count.reaction.emoticon
                                            count = str(reaction_count.count)
                                            comment_emoji_string += emoji + " " + count + " "

                                    comment_date_time = comment_message.date.strftime('%Y-%m-%d %H:%M:%S')

                                    comments_list.append({
                                        'Comment Content': comment_text,
                                    })
                            except Exception as e:
                                comments_list = []
                                print(f'Error processing comments: {e}')

                            # Process the main message
                            media = 'True' if message.media else 'False'
                            emoji_string = ''
                            if message.reactions:
                                for reaction_count in message.reactions.results:
                                    emoji = reaction_count.reaction.emoticon
                                    count = str(reaction_count.count)
                                    emoji_string += emoji + " " + count + " "

                            date_time = message.date.strftime('%Y-%m-%d %H:%M:%S')
                            cleaned_content = remove_unsupported_characters(message.text)
                            cleaned_comments_list = remove_unsupported_characters(json.dumps(comments_list))

                            data.append({
                                'Type': 'text',
                                'Content': cleaned_content,
                                'Comment': cleaned_comments_list,
                            })

                            c_index += 1
                            t_index += 1

                            # Print progress
                            print(f'{"-" * 80}')
                            print_progress(t_index, message.id, start_time, max_t_index)
                            current_max_id = min(c_index + message.id, max_t_index)
                            print(f'From {channel}: {c_index:05} contents of {current_max_id:05}')
                            print(f'Id: {message.id:05} / Date: {date_time}')
                            print(f'Total: {t_index:05} contents until now')
                            print(f'{"-" * 80}\n\n')

                            if t_index % 1000 == 0:
                                if file_format == 'parquet':
                                    backup_filename = f'backup_{file_name}_until_{t_index:05}_{channel}_ID{message.id:07}.parquet'
                                    pd.DataFrame(data).to_parquet(backup_filename, index=False)
                                elif file_format == 'excel':
                                    backup_filename = f'backup_{file_name}_until_{t_index:05}_{channel}_ID{message.id:07}.xlsx'
                                    pd.DataFrame(data).to_excel(backup_filename, index=False, engine='openpyxl')

                            if t_index >= max_t_index:
                                break

                            if time.time() - start_time > time_limit:
                                break

                        elif message.date < date_min:
                            break

                    except Exception as e:
                        print(f'Error processing message: {e}')

            print(f'\n\n##### {channel} was ok with {c_index:05} posts #####\n\n')

            df = pd.DataFrame(data)
            # if file_format == 'parquet':
            #     partial_filename = f'complete_{channel}_in_{file_name}_until_{t_index:05}.parquet'
            #     df.to_parquet(partial_filename, index=False)
            # elif file_format == 'excel':
            #     partial_filename = f'complete_{channel}_in_{file_name}_until_{t_index:05}.xlsx'
            #     df.to_excel(partial_filename, index=False, engine='openpyxl')

        except Exception as e:
            print(f'{channel} error: {e}')

        loop_end_time = time.time()
        loop_duration = loop_end_time - loop_start_time

        if loop_duration < 60:
            time.sleep(60 - loop_duration)

    print(f'\n{"-" * 50}\n#Concluded! #{t_index:05} posts were scraped!\n{"-" * 50}\n\n\n\n')
    df = pd.DataFrame(data)
    if file_format == 'parquet':
        final_filename = f'FINAL_{file_name}_with_{t_index:05}.parquet'
        df.to_parquet(final_filename, index=False)
    elif file_format == 'excel':
        final_filename = f'FINAL_{file_name}_with_{t_index:05}.xlsx'
        df.to_excel(final_filename, index=False, engine='openpyxl')

    return final_filename, df


# Streamlit interface
st.title("Telegram Scraper")

# Form for inputting credentials and parameters
with st.form(key='scraping_form'):
    col1, col2 = st.columns([1, 1])  # Split into two columns

    # Using smaller input boxes
    with col1:
        username = st.text_input('Telegram Username (without @)', 'zarifhaikalz', key='username', help="Enter your Telegram username without '@'")
        phone = st.text_input('Phone Number', '+601163246810', key='phone', help="Enter your phone number")
        api_id = st.text_input('API ID', '29133717', key='api_id', help="Enter your API ID")
        api_hash = st.text_input('API Hash', 'eeb307b992fadc85cf7d8200591697f4', key='api_hash', help="Enter your API Hash")

    with col2:
        # Scraping parameters
        channels = st.text_area('Enter Channels Name or link (comma separated)', '@LulanoTelegram, @jairbolsonarobrasil, @Other_Channel_Name', height=120, key='channels')
        date_min = st.date_input('Start Date', datetime.today(), key='date_min')
        date_max = st.date_input('End Date', datetime.today(), key='date_max')
        file_name = st.text_input('File Name', 'Scrape', key='file_name')
        key_search = st.text_input('Search Keyword (leave blank for all)', '', key='key_search')
        
        # Corrected number input usage
        max_t_index = st.number_input('Max Messages', min_value=1, max_value=100000000, value=1000)  # Setting a max_value
        time_limit = st.number_input('Time Limit (in seconds)', min_value=1, max_value=86400, value=21600)  # 1 day max limit

        file_format = st.selectbox('Output File Format', ['excel', 'parquet'])

    submit_button = st.form_submit_button(label='Start Scraping', use_container_width=True)

    # Convert dates
    date_min = datetime.combine(date_min, datetime.min.time()).replace(tzinfo=timezone.utc)
    date_max = datetime.combine(date_max, datetime.max.time()).replace(tzinfo=timezone.utc)

    # Parse the channels input
    channels = [channel.strip() for channel in channels.split(",")]

    # Call the scraping function asynchronously
    final_filename, df = asyncio.run(scrape_telegram(username, phone, api_id, api_hash, channels, date_min, date_max, file_name, key_search, max_t_index, time_limit, file_format))

    # Allow the user to download the file
    with open(final_filename, "rb") as f:
        st.download_button(
            label="Download Scraped Data",
            data=f,
            file_name=final_filename,
            mime="application/octet-stream",
            use_container_width=True,
            key='download_button'
        )
